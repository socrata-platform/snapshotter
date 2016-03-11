package com.socrata.snapshotter

import com.typesafe.config.ConfigFactory
import java.util.concurrent.Executors
import com.socrata.curator.{DiscoveryFromConfig, CuratorFromConfig}
import com.socrata.http.server.SocrataServerJetty
import com.socrata.http.client.HttpClientHttpClient
import com.rojoma.simplearm.v2._

object Snapshotter extends App {
    val config = new SnapshotterConfig(ConfigFactory.load())
    implicit val shutdownTimeout = Resource.executorShutdownNoTimeout
    for {
      executor <- managed(Executors.newCachedThreadPool())
      httpClient <- managed(new HttpClientHttpClient(executor, HttpClientHttpClient.defaultOptions.withUserAgent("snapshotter")))
      curator <- CuratorFromConfig(config.curator)
      discovery <- DiscoveryFromConfig(classOf[Void], curator, config.discovery)
      // broker is loaded with our service's config info and is able to create a connection with zookeeper
      // client (returned from zookeeper) is configured for making requests to core (specified in config file)
      client <- broker.clientFor(config.client)
    } {
      val broker = new CuratorBroker(discovery, config.address.getHostAddress, config.service.advertisement.service, None)
      val snapshotService = SnapshotService(client)
      val router = Router(versionService = VersionService,
                          snapshotService = snapshotService.takeSnapshotService,
                          snapshotServingService = snapshotService.fetchSnapshotService,
                          listService = ListService.service)
      val handler = router.route _

      val server = new SocrataServerJetty(
        handler = handler,
        options = SocrataServerJetty.defaultOptions.
          withPort(config.port).
          withOnStop(BlobStoreManager.shutdownManager)
      )

      server.run()
    }
}
