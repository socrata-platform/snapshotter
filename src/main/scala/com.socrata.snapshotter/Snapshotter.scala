package com.socrata.snapshotter

import com.socrata.curator.{CuratedClientConfig, DiscoveryBrokerConfig, DiscoveryBrokerFromConfig}
import com.typesafe.config.ConfigFactory

import com.socrata.http.server.SocrataServerJetty

object Snapshotter extends App {

   lazy val config = ConfigFactory.load().getConfig("com.socrata")

    for {
      // broker is loaded with our service's config info and is able to create a connection with zookeeper
      broker <- DiscoveryBrokerFromConfig(new DiscoveryBrokerConfig(config, "broker"), "snapshotter")
      // client (returned from zookeeper) is configured specifically for making requests to core (specified in config file)
      client <- broker.clientFor(new CuratedClientConfig(config, "upstream"))
    } {
      val router = Router(VersionService, SnapshotService(client).service)
      val handler = router.route _

      val server = new SocrataServerJetty(
        handler = handler,
        options = SocrataServerJetty.defaultOptions.withPort(6800)
      )

      server.run()
    }
}
