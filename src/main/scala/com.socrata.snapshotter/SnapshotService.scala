package com.socrata.snapshotter

import java.io.{InputStream, FileOutputStream, BufferedOutputStream, IOException}
import java.util.zip.{ZipEntry, ZipOutputStream}

import com.rojoma.json.v3.ast.{JString, JValue}
import com.rojoma.json.v3.interpolation._
import com.rojoma.simplearm.util._

import com.socrata.curator._
import com.socrata.http.client.{Response, SimpleHttpRequest, RequestBuilder}
import com.socrata.http.server.implicits._
import com.socrata.http.server.responses._
import com.socrata.http.server.routing.SimpleResource
import com.socrata.http.server.{HttpResponse, HttpRequest, HttpService}
import org.apache.commons.io.IOUtils
import org.slf4j.LoggerFactory

case class SnapshotService(client: CuratedServiceClient) extends SimpleResource {
  private val logger = LoggerFactory.getLogger(getClass)

  def handleRequest(req: HttpRequest, datasetId: String): HttpResponse = {

    val makeReq: RequestBuilder => SimpleHttpRequest = { base =>
      val host = req.header("X-Socrata-Host").map("X-Socrata-Host" -> _)

      val csvReq = base.
        addPaths(Seq("views", datasetId, "rows.cjson")).
        addHeaders(host).
        addParameter("accessType" -> "DOWNLOAD").get
      logger.info(csvReq.toString)

      csvReq
    }

    val bytes = client.execute(makeReq, saveExport)

    bytes match {
      case Right(b) =>
        OK ~> Content("text/plain", s"Successfully wrote $b bytes of dataset $datasetId.")
      case Left(msg) =>
        InternalServerError ~> Json(msg)
    }
  }

  def saveExport(resp: Response): Either[JValue, Long] = {
    if (resp.resultCode != 200) {
      val underlying = try {
        resp.jValue()
      } catch {
        case _: Exception =>
          JString(IOUtils.toString(resp.inputStream(), "UTF-8"))
      }

      val msg = json"""{ message: "Failed to export!", underlying: $underlying }"""
      logger.warn(msg.toString)
      return Left(msg)
    }

    try {
      Right(zipStream(resp.inputStream(), "/tmp/test.zip", "test.cjson"))
    } catch {
      case ex: IOException =>
        val msg = json"""{ message: "Failed to write file!", info: ${ex.getMessage} }"""
        logger.warn(msg.toString)
        Left(msg)
    }
  }

  def service(datasetId: String): HttpService = {
    new SimpleResource {
      override def get: HttpService = req =>
          handleRequest(req, datasetId)
    }
  }

  def zipStream(inStream: InputStream, path: String, filename: String): Long = {
    using(new ZipOutputStream(new BufferedOutputStream(new FileOutputStream(path)))) { zos =>
      zos.putNextEntry(new ZipEntry(filename))
      IOUtils.copyLarge(inStream, zos)
    }
  }



}
