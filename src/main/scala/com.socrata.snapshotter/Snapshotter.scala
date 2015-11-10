package com.socrata.snapshotter

import com.socrata.curator.DiscoveryBrokerFromConfig

import com.socrata.http.server.SocrataServerJetty

object Snapshotter extends App {

    for {
      // broker is loaded with our service's config info and is able to create a connection with zookeeper
      broker <- DiscoveryBrokerFromConfig(SnapshotterConfig.broker, "snapshotter")
      // client (returned from zookeeper) is configured for making requests to core (specified in config file)
      client <- broker.clientFor(SnapshotterConfig.client)
    } {
      val router = Router(VersionService, SnapshotService(client).service, ListService)
      val handler = router.route _

      val server = new SocrataServerJetty(
        handler = handler,
        options = SocrataServerJetty.defaultOptions.
          withPort(SnapshotterConfig.port).
          withOnStop(BlobManager.shutdownManager)
      )

      server.run()
    }
}
