package com.socrata.snapshotter

import com.socrata.http.server._
import com.socrata.http.server.implicits._
import com.socrata.http.server.responses._
import com.socrata.http.server.routing.SimpleRouteContext._
import com.socrata.http.server.routing.Extractor
import com.socrata.http.server.util.RequestId._
import com.socrata.http.server.util.handlers.{LoggingOptions, NewLoggingHandler}

import org.slf4j.LoggerFactory

case class ResourceName(underlying: String)
case class SnapshotName(name: String, gzipped: Boolean)
case class TimestampPrefix(prefix: String)

case class Router(versionService: HttpService,
                  snapshotService: ResourceName => HttpService,
                  snapshotServingService: (ResourceName, SnapshotName) => HttpService,
                  listService: (ResourceName, TimestampPrefix) => HttpService) {

  private val logger = LoggerFactory.getLogger(getClass)
  private val logWrapper =
    NewLoggingHandler(LoggingOptions(logger, Set("X-Socrata-Host", "X-Socrata-Resource", ReqIdHeader))) _

  private implicit val resourceNameExtractor =
    new Extractor[ResourceName] {
      override def extract(s: String) = Some(ResourceName(s))
    }

  val timestampRegexStr = """\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d*)?Z"""
  val TimestampRegex = s"""($timestampRegexStr)""".r
  val SnapshotNameRegex = s"""($timestampRegexStr)\\.csv(\\.gz)?""".r

  private implicit val snapshotNameExtractor =
    new Extractor[SnapshotName] {
      override def extract(s: String) = s match {
          case SnapshotNameRegex(timestamp, gz) => Some(SnapshotName(timestamp, gz != null))
          case _ => None
        }
    }

  private implicit val timestampPrefixExtractor = new Extractor[TimestampPrefix] {
      // ok, this is a little bit annoying.  We want to find accept anything that's
      // a valid _prefix_  of a timestamp.  Fortunately, they've got fixed-position
      // fields... except for the fractions-of-seconds bit.
      private val fixedPart = "0000-00-00T00:00:00"
      private val FracSuffix = """\.\d*Z?""".r

      override def extract(s: String): Option[TimestampPrefix] = {
        if(s.length <= fixedPart.length) extractNoFrac(s)
        else extractWithFrac(s)
      }

      private def extractNoFrac(s: String): Option[TimestampPrefix] = {
        val effective = s + fixedPart.drop(s.length) + "Z"
        effective match {
          case TimestampRegex(_) => Some(TimestampPrefix(s))
          case _ => None
        }
      }

      private def extractWithFrac(s: String): Option[TimestampPrefix] = {
        extractNoFrac(s.take(fixedPart.length)).flatMap { _ =>
          // ok, first part is good.  So now we're looking for some prefix of "(\.\d*)Z"
          val remainder = s.drop(fixedPart.length) // this is certain to be non-empty
          remainder.charAt(0) match {
            case '.' =>
              remainder match {
                case FracSuffix() => Some(TimestampPrefix(s))
                case _ => None
              }
            case 'Z' if remainder.length == 1 =>
              Some(TimestampPrefix(s))
            case _ =>
              None
          }
        }
      }
    }

  val routes = Routes(
    Route("/version", versionService),
    Route("/snapshot/{ResourceName}", snapshotService),
    Route("/snapshot/{ResourceName}/{SnapshotName}", snapshotServingService),
    Route("/list/{ResourceName}", listService(_ : ResourceName, TimestampPrefix(""))),
    Route("/list/{ResourceName}/{TimestampPrefix}", listService)
  )

  def notFound(req: HttpRequest): HttpResponse = {
    logger.warn("path not found: {}", req.requestPathStr)
    NotFound ~> Content("text/plain", "Nothing found :(")
  }

  def route(req: HttpRequest): HttpResponse =
    logWrapper(routes(req.requestPath).getOrElse(notFound _))(req)
}
