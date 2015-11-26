package com.socrata.snapshotter

import java.io.InputStream

import com.rojoma.json.v3.ast.JValue

import com.socrata.http.server.{HttpResponse, HttpRequest, HttpService}
import com.socrata.http.server.routing.SimpleResource
import com.socrata.http.server.responses._
import com.socrata.http.server.implicits._

import org.slf4j.LoggerFactory

object RetrievalService extends SimpleResource {
  private val logger = LoggerFactory.getLogger(getClass)

  def service(fileKey: String): HttpService = {
    new SimpleResource {
      override def get: HttpService = req =>
        handleRequest(req, fileKey)
    }
  }

  def handleRequest(req: HttpRequest, fileKey: String): HttpResponse = {
    val fileStream = BlobStoreManager.getFile(fileKey, req.resourceScope)
    OK ~> ContentType("text/csv") ~> Stream(fileStream)
  }
}
