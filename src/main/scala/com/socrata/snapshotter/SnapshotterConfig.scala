package com.socrata.snapshotter

import com.socrata.curator.{CuratorConfig, CuratedClientConfig, DiscoveryBrokerConfig, DiscoveryConfig}
import com.socrata.thirdparty.typesafeconfig.ConfigClass
import com.typesafe.config.Config

class SnapshotterServiceConfig(config: Config) extends ConfigClass(config, "com.socrata") {
  val snapshotter = getConfig("snapshotter", new SnapshotterConfig(_,_))
  val aws = getConfig("aws", new AwsConfig(_, _))

  val curator = getConfig("curator", new CuratorConfig(_, _))
  val advertisement = getConfig("advertisement", new DiscoveryConfig(_, _))
  val core = getConfig("core", new CuratedClientConfig(_, _))
  val sodaFountain = getConfig("soda-fountain", new CuratedClientConfig(_, _))
}

class SnapshotterConfig(config: Config, root: String) extends ConfigClass(config, root) {
  val port = getInt("port")
  val gzipBufferSize = getInt("gzip-buffer-size")
  val poll = getConfig("snapshot-poll", new SnapshotPollConfig(_, _))
}

class AwsConfig(config: Config, root: String) extends ConfigClass(config, root) {
  val uploadPartSize = getInt("upload-part-size")
  val bucketName = getString("bucket-name")
}

class SnapshotPollConfig(config: Config, root: String) extends ConfigClass(config, root) {
  val interval = getDuration("interval")
  val latchRoot = getString("latch-root")
}
