package com.socrata.snapshotter

import com.rojoma.json.v3.ast.JValue
import com.socrata.curator.CuratedServiceClient
import com.socrata.http.server.{HttpResponse, HttpRequest, HttpService}

import com.socrata.http.server.responses._
import com.socrata.http.server.implicits._
import com.socrata.http.server.routing.SimpleResource

import org.slf4j.LoggerFactory

case class ListService(client: CuratedServiceClient) extends SimpleResource {
  private val logger = LoggerFactory.getLogger(getClass)

  def service(datasetId: String): HttpService = {
    new SimpleResource {
      override def get: HttpService = req =>
        handleRequest(req, datasetId)
    }
  }

  def handleRequest(req: HttpRequest, datasetId: String): HttpResponse = {
    val resp = requestList(datasetId)
    OK ~> Content("application/json", s"$resp" )
  }

  def requestList(datasetId: String): JValue = {
    BlobStoreManager.listObjects(SnapshotterConfig.awsBucketName, datasetId)
  }

}
