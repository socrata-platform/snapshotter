package com.socrata.snapshotter

import com.typesafe.config.ConfigFactory
import java.util.concurrent.Executors
import com.socrata.curator.{CuratorBroker => _, _}
import com.socrata.http.server.SocrataServerJetty
import com.socrata.http.client.HttpClientHttpClient
import com.socrata.http.server.curator.CuratorBroker
import com.rojoma.simplearm.v2._

object Snapshotter extends App {
  val config = new SnapshotterServiceConfig(ConfigFactory.load())
  implicit val shutdownTimeout = Resource.executorShutdownNoTimeout

  for {
    executor <- managed(Executors.newCachedThreadPool())
    httpClient <- managed(new HttpClientHttpClient(executor, HttpClientHttpClient.defaultOptions.withUserAgent("snapshotter")))
    curator <- CuratorFromConfig(config.curator)
    discovery <- DiscoveryFromConfig(classOf[Void], curator, config.advertisement)
    coreServiceProvider <- ServiceProviderFromName(discovery, config.core.serviceName)
    sfServiceProvider <- ServiceProviderFromName(discovery, config.sodaFountain.serviceName)
    blobStoreManager <- managed(new BlobStoreManager(config.aws.bucketName, config.aws.uploadPartSize))
  } {
    val coreClient = CuratedServiceClient(CuratorServerProvider(httpClient, coreServiceProvider, identity), config.core)
    val sfClient = CuratedServiceClient(CuratorServerProvider(httpClient, sfServiceProvider, identity), config.sodaFountain)
    val broker = new CuratorBroker(discovery, config.advertisement.address, config.advertisement.name, None)
    val snapshotService = SnapshotService(coreClient, blobStoreManager, config.snapshotter.gzipBufferSize)
    val router = Router(versionService = VersionService,
                        snapshotService = snapshotService.takeSnapshotService,
                        snapshotServingService = snapshotService.fetchSnapshotService,
                        listService = new ListService(blobStoreManager).service)
    val handler = router.route _
    val snapshotDAO = new SnapshotDAOImpl(sfClient)

    using(new SodaWatcher(curator, config.snapshotter.poll.latchRoot, config.snapshotter.poll.interval, snapshotDAO)) { sw =>
      sw.start()
      val server = new SocrataServerJetty(
        handler = handler,
        options = SocrataServerJetty.defaultOptions.
          withPort(config.snapshotter.port).
          withBroker(broker)
      )

      server.run()
    }
  }
}
