package com.socrata.snapshotter

import com.socrata.http.server.implicits._
import com.socrata.http.server.responses._
import com.socrata.http.server.routing.SimpleResource

object VersionService extends SimpleResource {
  override def get = { req =>
    OK ~> Content("application/json", "Meow!")
  }
}
