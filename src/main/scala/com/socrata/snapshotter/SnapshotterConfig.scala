package com.socrata.snapshotter

import com.socrata.curator.{CuratorConfig, CuratedClientConfig, DiscoveryBrokerConfig, DiscoveryConfig}
import com.socrata.thirdparty.typesafeconfig.ConfigClass
import com.typesafe.config.Config

class SnapshotterServiceConfig(config: Config) extends ConfigClass(config, "com.socrata") {
  val snapshotter = new SnapshotterConfig(config, "snapshotter")
  val aws = new AwsConfig(config, "aws")

  val curator = new CuratorConfig(config, "curator")
  val advertisement = new DiscoveryConfig(config, "advertisement")
  val core = new CuratedClientConfig(config, "core")
  val sodaFountain = new CuratedClientConfig(config, "soda-fountain")

}

class SnapshotterConfig(config: Config, root: String) extends ConfigClass(config, root) {
  val port = getInt("port")
  val gzipBufferSize = getInt("gzip-buffer-size")
}

class AwsConfig(config: Config, root: String) extends ConfigClass(config, root) {
  val uploadPartSize = getInt("upload-part-size")
  val bucketName = config.getString("bucket-name")
}
