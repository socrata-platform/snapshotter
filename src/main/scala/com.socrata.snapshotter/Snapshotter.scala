package com.socrata.snapshotter

import com.socrata.http.server.implicits._
import com.socrata.http.server.responses._
import com.socrata.http.server.routing.SimpleRouteContext.{Route, Routes}
import com.socrata.http.server.{SocrataServerJetty, HttpResponse, HttpRequest, HttpService}

object Snapshotter extends App {
  case class Router(versionService: HttpService) {
    val routes = Routes(
      Route("/version", versionService))

    def notFound(req: HttpRequest): HttpResponse =
       NotFound ~> Content("text/plain", "Nothing found :(")

    def route(req: HttpRequest): HttpResponse =
      routes(req.requestPath).getOrElse(notFound _)(req)
  }

  val router = Router(VersionService)
  val handler = router.route _

  val server = new SocrataServerJetty(
    handler = handler,
    options = SocrataServerJetty.defaultOptions.withPort(6800)
  )

  server.run()
}
