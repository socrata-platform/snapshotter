package com.socrata.snapshotter

import org.joda.time.DateTime
import com.rojoma.json.v3.codec.JsonEncode
import buildinfo.BuildInfo

import com.socrata.http.server.implicits._
import com.socrata.http.server.responses._
import com.socrata.http.server.routing.SimpleResource

object VersionService extends SimpleResource {
  override def get = { req =>
    OK ~> Json(JsonEncode.toJValue(
                Map("version" -> BuildInfo.version,
                    "scalaVersion" -> BuildInfo.scalaVersion,
                    "buildTime" -> new DateTime(BuildInfo.buildTime).toString)))
  }
}
