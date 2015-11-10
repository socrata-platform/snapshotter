package com.socrata.snapshotter

import java.io.InputStream

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

case class SnapshotService(client: CuratedServiceClient) extends SimpleResource {
  private val logger = LoggerFactory.getLogger(getClass)
  private val gzipBufferSize = 4096

  def handleRequest(req: HttpRequest, datasetId: String): HttpResponse = {

    val makeReq: RequestBuilder => SimpleHttpRequest = { base =>
      val host = req.header("X-Socrata-Host").map("X-Socrata-Host" -> _)
      val csvReq = base.
        addPaths(Seq("views", datasetId, "rows.csv")).
        addHeaders(host).
        addParameter("accessType" -> "DOWNLOAD").get
      logger.info(csvReq.toString())
      csvReq
    }

    val response = client.execute(makeReq, saveExport(datasetId))

    // need to catch response signifying error
    response match {
      case Right(ur) =>
        OK ~> Content("text/plain", s"Successfully wrote dataset $datasetId, to ${ur.getKey}")
      case Left(msg) =>
        InternalServerError ~> Json(msg)
    }
  }

  def saveExport(datasetId: String): Response => Either[JValue, UploadResult] = { resp: Response =>

    if (resp.resultCode == 200) {
      val now = new DateTime(DateTimeZone.forID("UTC"))
      using(new GZipCompressInputStream(resp.inputStream(), gzipBufferSize)) { inStream =>
        upload(inStream, s"/$datasetId/$datasetId-$now.zip")
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

  def upload(inStream: InputStream, path: String): Either[JValue, UploadResult] = {
    try {
      Right(BlobManager.upload(inStream, path))
    } catch {
      case exception: AmazonS3Exception => Left(
        json"""{ message: "Problem uploading to S3",
                     error: ${exception.toString},
                     "error code": ${exception.getErrorCode},
                     "error type": ${exception.getErrorType},
                     "error message": ${exception.getErrorMessage} }""")
    }
  }

  def service(datasetId: String): HttpService = {
    new SimpleResource {
      override def get: HttpService = req =>
          handleRequest(req, datasetId)
    }
  }
}
