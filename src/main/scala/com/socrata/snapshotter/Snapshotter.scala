package com.socrata.snapshotter

import com.typesafe.config.ConfigFactory
import java.util.concurrent.Executors
import com.socrata.curator.{CuratorBroker => _, _}
import com.socrata.http.server.SocrataServerJetty
import com.socrata.http.client.HttpClientHttpClient
import com.socrata.http.server.curator.CuratorBroker
import com.rojoma.simplearm.v2._

object Snapshotter extends App {
  val config = new SnapshotterConfig(ConfigFactory.load())
  implicit val shutdownTimeout = Resource.executorShutdownNoTimeout

  // Ok, let's just redo ALL THE THINGS.
  // What we need:
  //  * executor
  //  * http client
  //  * discovery for core
  //  * discovery for soda-fountain
  //  * broker for our own advertisement
  for {
    executor <- managed(Executors.newCachedThreadPool())
    httpClient <- managed(new HttpClientHttpClient(executor, HttpClientHttpClient.defaultOptions.withUserAgent("snapshotter")))
    curator <- CuratorFromConfig(config.curator)
    discovery <- DiscoveryFromConfig(classOf[Void], curator, config.advertisement)
    coreServiceProvider <- ServiceProviderFromName(discovery, config.core.serviceName)
    sfServiceProvider <- ServiceProviderFromName(discovery, config.sodaFountain.serviceName)
    blobStoreManager <- managed(new BlobStoreManager(config.awsBucketName, config.uploadPartSize))
  } {
    val coreClient = CuratedServiceClient(CuratorServerProvider(httpClient, coreServiceProvider, identity), config.core)
    val sfClient = CuratedServiceClient(CuratorServerProvider(httpClient, sfServiceProvider, identity), config.sodaFountain)
    val broker = new CuratorBroker(discovery, config.advertisement.address, config.advertisement.name, None)
    val snapshotService = SnapshotService(coreClient, blobStoreManager, config.gzipBufferSize)
    val router = Router(versionService = VersionService,
                        snapshotService = snapshotService.takeSnapshotService,
                        snapshotServingService = snapshotService.fetchSnapshotService,
                        listService = new ListService(blobStoreManager).service)
    val handler = router.route _

    val server = new SocrataServerJetty(
      handler = handler,
      options = SocrataServerJetty.defaultOptions.
        withPort(config.port).
        withBroker(broker)
    )

    server.run()
  }
}
