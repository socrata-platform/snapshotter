package com.socrata.snapshotter

import com.rojoma.json.v3.ast.JValue
import com.socrata.http.server.{HttpResponse, HttpRequest, HttpService}

import com.socrata.http.server.responses._
import com.socrata.http.server.implicits._
import com.socrata.http.server.routing.SimpleResource

import org.slf4j.LoggerFactory

class ListService(blobStoreManager: BlobStoreManager) extends SimpleResource {
  private val logger = LoggerFactory.getLogger(getClass)

  def service(datasetId: DatasetId, timestampPrefix: TimestampPrefix): HttpService = {
    new SimpleResource {
      override def get: HttpService = req =>
        handleRequest(req, datasetId, timestampPrefix)
    }
  }

  def handleRequest(req: HttpRequest, datasetId: DatasetId, prefix: TimestampPrefix): HttpResponse = {
    val resp = requestList(datasetId, prefix)
    OK ~> Content("application/json", s"$resp" )
  }

  def requestList(datasetId: DatasetId, prefix: TimestampPrefix): JValue = {
    blobStoreManager.listObjects(datasetId.uid + "-" + prefix.prefix)
  }

}
