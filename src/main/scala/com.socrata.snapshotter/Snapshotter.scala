package com.socrata.snapshotter

import org.slf4j.LoggerFactory

import com.socrata.http.server.implicits._
import com.socrata.http.server.responses._
import com.socrata.http.server.util.RequestId.ReqIdHeader
import com.socrata.http.server.util.handlers.{LoggingOptions, NewLoggingHandler}
import com.socrata.http.server.routing.SimpleRouteContext.{Route, Routes}
import com.socrata.http.server.{SocrataServerJetty, HttpResponse, HttpRequest, HttpService}

object Snapshotter extends App {

  case class Router(versionService: HttpService,
                    snapshotService: (String) => HttpService) {

    private val logger = LoggerFactory.getLogger(getClass)
    private val logWrapper =
      NewLoggingHandler(LoggingOptions(logger, Set("X-Socrata-Host", "X-Socrata-Resource", ReqIdHeader))) _
    val routes = Routes(
      Route("/version", versionService),
      Route("/snapshot/{String}", snapshotService)
    )

    def notFound(req: HttpRequest): HttpResponse =
       NotFound ~> Content("text/plain", "Nothing found :(")

    def route(req: HttpRequest): HttpResponse =
      routes(req.requestPath).getOrElse(notFound _)(req)
  }

  val router = Router(VersionService, SnapshotService.service)
  val handler = router.route _

  val server = new SocrataServerJetty(
    handler = handler,
    options = SocrataServerJetty.defaultOptions.withPort(6800)
  )

  server.run()
}
