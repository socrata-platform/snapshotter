package com.socrata.snapshotter

import com.socrata.http.server._
import com.socrata.http.server.implicits._
import com.socrata.http.server.responses._
import com.socrata.http.server.routing.SimpleRouteContext._
import com.socrata.http.server.util.RequestId._
import com.socrata.http.server.util.handlers.{LoggingOptions, NewLoggingHandler}

import org.slf4j.LoggerFactory

case class Router(versionService: HttpService,
                  snapshotService: (String) => HttpService,
                  listService: (String) => HttpService,
                   retrievalService: (String) => HttpService) {

  private val logger = LoggerFactory.getLogger(getClass)
  private val logWrapper =
    NewLoggingHandler(LoggingOptions(logger, Set("X-Socrata-Host", "X-Socrata-Resource", ReqIdHeader))) _

  val routes = Routes(
    Route("/version", versionService),
    Route("/snapshot/{String}", snapshotService),
    Route("/list/{String}", listService),
    Route("/export/{String}", retrievalService)
  )

  def notFound(req: HttpRequest): HttpResponse = {
    logger.warn("path not found: {}", req.requestPathStr)
    NotFound ~> Content("text/plain", "Nothing found :(")
  }

  def route(req: HttpRequest): HttpResponse =
    logWrapper(routes(req.requestPath).getOrElse(notFound _))(req)
}
