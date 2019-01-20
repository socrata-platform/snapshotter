package com.socrata.snapshotter

import java.io.InputStream
import java.util.zip.GZIPInputStream

import com.rojoma.json.v3.ast.{JString, JValue}
import com.rojoma.json.v3.interpolation._
import com.rojoma.simplearm.v2.using
import com.socrata.curator._
import com.socrata.http.client.{RequestBuilder, Response, SimpleHttpRequest}
import com.socrata.http.server.implicits._
import com.socrata.http.server.responses._
import com.socrata.http.server.routing.SimpleResource
import com.socrata.http.server.util.RequestId
import com.socrata.http.server.{HttpRequest, HttpResponse, HttpService}
import org.joda.time.{DateTime, DateTimeZone}
import org.apache.commons.io.IOUtils
import org.slf4j.LoggerFactory

case class SnapshotService(sfClient: CuratedServiceClient, blobStoreManager: BlobStoreManager, gzipBufferSize: Int, basenameFor: (ResourceName, DateTime) => String) extends SimpleResource {
  private val logger = LoggerFactory.getLogger(getClass)

  def handleSnapshotRequest(req: HttpRequest, resourceName: ResourceName): HttpResponse = {
    val phase = req.queryParameter("stage").getOrElse("latest")
    val makeReq: RequestBuilder => SimpleHttpRequest = { base =>
      val csvReq = base.
        addPaths(Seq("export", resourceName.underlying, phase + ".csv")).
        addHeader(RequestId.ReqIdHeader -> req.requestId).
        get
      logger.debug(csvReq.toString())
      csvReq
    }

    val response = sfClient.execute(makeReq, saveExport(resourceName))

    // need to catch response signifying error
    response match {
      case Right(timestamp) =>
        OK ~> Json(timestamp.withZone(DateTimeZone.UTC).toString)
      case Left(msg) =>
        InternalServerError ~> Json(msg)
    }
  }

  def handleSnapshotRequestFromReqPayload(req: HttpRequest, resourceName: ResourceName, dateTime: DateTime): HttpResponse = {
    val result = saveExport(resourceName, dateTime, req.servletRequest.getInputStream)
    result match {
      case Right(timestamp) =>
        OK ~> Json(timestamp.withZone(DateTimeZone.UTC).toString)
      case Left(msg) =>
        InternalServerError ~> Json(msg)
    }
  }

  def saveExport(resourceName: ResourceName, dateTime: DateTime, inputStream: InputStream): Either[JValue, DateTime] = {
    val basename = basenameFor(resourceName, dateTime)

    using(new ThreadlessGZipCompressInputStream(inputStream, gzipBufferSize)) { inStream =>
      logger.info(s"About to start multipart upload request for dataset ${resourceName.underlying}")
      blobStoreManager.multipartUpload(inStream, s"$basename.csv.gz").right.map { _ =>
        dateTime
      }
    }
  }

  def saveExport(resourceName: ResourceName): Response => Either[JValue, DateTime] = { resp: Response =>

    if (resp.resultCode == 200) {
      val now = new DateTime(DateTimeZone.UTC)
      val basename = basenameFor(resourceName, now)

//      Debug by downloading a file locally
//      val zipped = new GZipCompressInputStream(resp.inputStream(), gzipBufferSize)
//      val chunked = new StreamChunker(zipped, gzipBufferSize)
//      val tempFile = new FileOutputStream("/tmp/chunked.csv.gz", false)
//      chunked.foreach { case (chunk, _) => IOUtils.copy(chunk, tempFile) }
//      Left(JString("Saved a file!"))

      using(new ThreadlessGZipCompressInputStream(resp.inputStream(), gzipBufferSize)) { inStream =>
        logger.info(s"About to start multipart upload request for dataset ${resourceName.underlying}")
        blobStoreManager.multipartUpload(inStream, s"$basename.csv.gz").right.map { _ =>
          now
        }
       }
    } else {
      Left(extractErrorMsg(resp))
    }
  }

  def extractErrorMsg(resp: Response): JValue = {
    val underlying = try {
      resp.jValue()
    } catch {
      case _: Exception => JString(IOUtils.toString(resp.inputStream(), "UTF-8"))
    }

    val msg = json"""{ message: "Failed to export!", underlying: $underlying }"""
    logger.warn(msg.toString())
    msg
  }

  def handleFetchRequest(req: HttpRequest, resourceName: ResourceName, name: SnapshotName): HttpResponse = {
    val basename = basenameFor(resourceName, name.timestamp)
    blobStoreManager.fetch(s"${basename}.csv.gz", req.resourceScope) match {
      case Some(s3Object) =>
        if(name.gzipped) {
          ContentLength(s3Object.getObjectMetadata.getContentLength) ~> Stream { out =>
            IOUtils.copy(s3Object.getObjectContent, out)
          }
        } else if(acceptGzip(req)) {
          // no content-length because it is unclear if it should be the CL of the compressed data
          // or the uncompressed data.
          Header("Content-encoding", "gzip") ~> Stream { out =>
            IOUtils.copy(s3Object.getObjectContent, out)
          }
        } else {
          Stream { out =>
            using(new GZIPInputStream(s3Object.getObjectContent)) { decompressed =>
              IOUtils.copy(decompressed, out)
            }
          }
        }
      case None =>
        NotFound
    }
  }

  def handleDeleteRequest(req: HttpRequest, resourceName: ResourceName, name: SnapshotName): HttpResponse = {
    val basename = basenameFor(resourceName, name.timestamp)
    blobStoreManager.delete(s"${basename}.csv.gz", req.resourceScope)
    // Return OK, never NotFound, because DELETE should be idempotent
    OK
  }

  def acceptGzip(req: HttpRequest) = req.header("accept-encoding").fold(false)(_.contains("gzip"))

  def takeSnapshotService(resourceName: ResourceName): HttpService = {
    new SimpleResource {
      override def get: HttpService = req =>
          handleSnapshotRequest(req, resourceName)
      override def post: HttpService = req =>
          Option(req.servletRequest.getParameter("datetime")) match {
            case Some(dt) =>
              val dateTime = DateTime.parse(dt)
              // This is a "temporary" method to support moving obe snapshot to new backup format for nbe-only datasets
              // w/o waiting for the natural publication cycle
              // csv content is coming from the request payload (vs calling soda fountain)
              handleSnapshotRequestFromReqPayload(req, resourceName, dateTime)
            case None =>
              handleSnapshotRequest(req, resourceName)
          }
    }
  }

  def fetchSnapshotService(resourceName: ResourceName, name: SnapshotName): HttpService =
    new SimpleResource {
      override def get: HttpService = handleFetchRequest(_, resourceName, name)
      override def delete: HttpService = handleDeleteRequest(_, resourceName, name)
    }
}
