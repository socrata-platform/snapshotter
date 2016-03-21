package com.socrata.snapshotter

import com.rojoma.json.v3.ast.JValue
import com.socrata.http.server.{HttpResponse, HttpRequest, HttpService}

import com.socrata.http.server.responses._
import com.socrata.http.server.implicits._
import com.socrata.http.server.routing.SimpleResource

import org.slf4j.LoggerFactory

class ListService(blobStoreManager: BlobStoreManager) extends SimpleResource {
  private val logger = LoggerFactory.getLogger(getClass)

  def service(resourceName: ResourceName, timestampPrefix: TimestampPrefix): HttpService = {
    new SimpleResource {
      override def get: HttpService = req =>
        handleRequest(req, resourceName, timestampPrefix)
    }
  }

  def handleRequest(req: HttpRequest, resourceName: ResourceName, prefix: TimestampPrefix): HttpResponse = {
    val resp = requestList(resourceName, prefix)
    OK ~> Json(resp)
  }

  def requestList(resourceName: ResourceName, prefix: TimestampPrefix): JValue = {
    blobStoreManager.listObjects(resourceName.underlying + ":" + prefix.prefix)
  }

}
