package com.socrata.snapshotter

import java.io.{ByteArrayInputStream, FileOutputStream, InputStream}
import java.nio.file.{StandardCopyOption, Files, Paths}
import java.util.zip.GZIPInputStream

import com.amazonaws.services.s3.model._
import com.amazonaws.services.s3.transfer.model.UploadResult

import com.rojoma.json.v3.ast.{JString, JValue}
import com.rojoma.json.v3.interpolation._
import com.rojoma.simplearm.v2.using

import com.socrata.curator._
import com.socrata.http.client.{Response, SimpleHttpRequest, RequestBuilder}
import com.socrata.http.server.implicits._
import com.socrata.http.server.responses._
import com.socrata.http.server.routing.SimpleResource
import com.socrata.http.server.{HttpResponse, HttpRequest, HttpService}

import org.joda.time.{DateTime, DateTimeZone}
import org.apache.commons.io.IOUtils
import org.slf4j.LoggerFactory

case class SnapshotService(coreClient: CuratedServiceClient, blobStoreManager: BlobStoreManager, gzipBufferSize: Int, basenameFor: (DatasetId, DateTime) => String) extends SimpleResource {
  private val logger = LoggerFactory.getLogger(getClass)

  def handleSnapshotRequest(req: HttpRequest, datasetId: DatasetId): HttpResponse = {

    val makeReq: RequestBuilder => SimpleHttpRequest = { base =>
      val host = req.header("X-Socrata-Host").map("X-Socrata-Host" -> _)
      val csvReq = base.
        addPaths(Seq("views", datasetId.uid, "rows.csv")).
        addHeaders(host).
        addParameter("accessType" -> "DOWNLOAD").get
      logger.debug(csvReq.toString())
      csvReq
    }

    val response = coreClient.execute(makeReq, saveExport(datasetId))

    // need to catch response signifying error
    response match {
      case Right(ur) =>
        OK ~> Content("text/plain", s"Successfully wrote dataset ${datasetId.uid}, to ${ur.getKey}")
      case Left(msg) =>
        InternalServerError ~> Json(msg)
    }
  }

  def saveExport(datasetId: DatasetId): Response => Either[JValue, CompleteMultipartUploadResult] = { resp: Response =>

    if (resp.resultCode == 200) {
      val now = new DateTime(DateTimeZone.UTC)
      val basename = basenameFor(datasetId, now)

//      Debug by downloading a file locally
//      val zipped = new GZipCompressInputStream(resp.inputStream(), gzipBufferSize)
//      val chunked = new StreamChunker(zipped, gzipBufferSize)
//      val tempFile = new FileOutputStream("/tmp/chunked.csv.gz", false)
//      chunked.foreach { case (chunk, _) => IOUtils.copy(chunk, tempFile) }
//      Left(JString("Saved a file!"))

      using(new GZipCompressInputStream(resp.inputStream(), gzipBufferSize)) { inStream =>
        logger.info(s"About to start multipart upload request for dataset ${datasetId.uid}")
        blobStoreManager.multipartUpload(inStream, s"$basename.csv.gz")
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


  def handleFetchRequest(req: HttpRequest, datasetId: DatasetId, name: SnapshotName): HttpResponse = {
    blobStoreManager.fetch(s"${datasetId.uid}-${name.name}.csv.gz", req.resourceScope) match {
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

  def acceptGzip(req: HttpRequest) = req.header("accept-encoding").fold(false)(_.contains("gzip"))

  def takeSnapshotService(datasetId: DatasetId): HttpService = {
    new SimpleResource {
      override def get: HttpService = req =>
          handleSnapshotRequest(req, datasetId)
      override def post: HttpService = req =>
          handleSnapshotRequest(req, datasetId)
    }
  }

  def fetchSnapshotService(datasetId: DatasetId, name: SnapshotName): HttpService =
    new SimpleResource {
      override def get: HttpService = handleFetchRequest(_, datasetId, name)
    }
}
