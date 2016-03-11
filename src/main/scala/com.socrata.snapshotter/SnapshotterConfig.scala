package com.socrata.snapshotter

import com.socrata.curator.{CuratorConfig, CuratedClientConfig, DiscoveryBrokerConfig, DiscoveryConfig}
import com.socrata.thirdparty.typesafeconfig.ConfigClass
import com.typesafe.config.Config

class SnapshotterConfig(config: Config) extends ConfigClass(config, "com.socrata") {
  lazy val port = getInt("snapshotter.port")
  lazy val gzipBufferSize = getInt("snapshotter.gzip-buffer-size")
  lazy val uploadPartSize = getInt("aws.upload-part-size")

  lazy val curator = new CuratorConfig(config, "curator")
  lazy val discovery = new DiscoveryConfig(config, "advertisement")
  lazy val client = new CuratedClientConfig(config, "upstream")

  lazy val awsBucketName = config.getString("aws.bucket-name")
}
