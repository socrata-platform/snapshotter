package com.socrata.snapshotter

import java.nio.file.{StandardCopyOption, Paths, Files}

import com.socrata.curator._
import com.socrata.http.client.{Response, SimpleHttpRequest, RequestBuilder}
import com.socrata.http.server.implicits._
import com.socrata.http.server.responses._
import com.socrata.http.server.routing.SimpleResource
import com.socrata.http.server.{HttpResponse, HttpRequest, HttpService}
import com.typesafe.config.ConfigFactory
import org.slf4j.LoggerFactory

// /api/views/datasetId/rows.csv?accessType=DOWNLOAD
object SnapshotService extends SimpleResource {
  private val logger = LoggerFactory.getLogger(getClass)

  def handleRequest(req: HttpRequest, datasetId: String): HttpResponse = {

    val makeReq: RequestBuilder => SimpleHttpRequest = { base =>
      val host = req.header("X-Socrata-Host").map("X-Socrata-Host" -> _)

      logger.info("Host: {}", host)

      val csvReq = base.
        addPaths(Seq("views", datasetId, "rows.csv")).
        addHeaders(host).
        addParameter("accessType" -> "DOWNLOAD").get
      logger.info(csvReq.toString)

      csvReq
    }

    lazy val config = ConfigFactory.load().getConfig("com.socrata")

    for {
      // broker is loaded with our service's config info and is able to create a connection with zookeeper
      broker <- DiscoveryBrokerFromConfig(new DiscoveryBrokerConfig(config, "broker"), "snapshotter")
      // client (returned from zookeeper) is configured specifically for making requests to core (specified in config file)
      client <- broker.clientFor(new CuratedClientConfig(config, "upstream"))
    } {
      client.execute(makeReq, saveExport)
    }

     OK ~> Content("application/json", datasetId)
  }

  def saveExport(resp: Response): Unit = {
    Files.copy(resp.inputStream(), Paths.get("/tmp/test.csv"), StandardCopyOption.REPLACE_EXISTING)
  }

  def service(datasetId: String): HttpService = {
    new SimpleResource {
      override def get: HttpService = req =>
          handleRequest(req, datasetId)
    }
  }

}
