package com.socrata.snapshotter

import java.io.{IOException, Closeable}

import com.rojoma.simplearm.v2._
import org.joda.time.DateTime

import scala.concurrent.duration.FiniteDuration
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.leader.LeaderLatch
import org.slf4j.LoggerFactory

import scala.util.control.Breaks

class SodaWatcher(curatorFramework: CuratorFramework,
                  latchPath: String,
                  pause: FiniteDuration,
                  snapshotDAO: SnapshotDAO,
                  gzipBufferSize: Int,
                  blobStoreManager: BlobStoreManager,
                  basenameFor: (ResourceName, DateTime) => String) extends Closeable {
  private val log = LoggerFactory.getLogger(classOf[SodaWatcher])
  private val latch = new LeaderLatch(curatorFramework, latchPath)
  private val pauseMS = pause.toMillis
  private var worker: Worker = null

  private val Quota = 3 // how many snapshots we keep in S3

  def start(): Unit =
    synchronized {
      if(worker == null) {
        worker = new Worker
        worker.start()
        latch.start()
      }
    }

  def close(): Unit =
    synchronized {
      if(worker != null) {
        latch.close()
        worker.interrupt() // pls shut down now
        worker.join()
      }
    }

  private class Worker extends Thread {
    override def run(): Unit = {
      while(true) {
        latch.await()
        try {
          poll()
        } catch {
          case e: Exception =>
            log.warn("Unexpected exception when polling snapshots", e)
            // and, uh, swallow it and keep trying, I guess?
        }
      }
    }

    private def poll(): Unit = {
      while(true) {
        Thread.sleep(pauseMS)
        if(!latch.hasLeadership) { return }
        for(resourceName <- snapshotDAO.datasetsWithSnapshots().par) {
          if(!latch.hasLeadership) { return }
          try {
            val b = new Breaks
            import b._
            breakable {
              for(snapshot <- snapshotDAO.datasetSnapshots(resourceName)) {
                if(!latch.hasLeadership) { return }
                log.info("Exporting snapshot {} on dataset {}", snapshot, resourceName.underlying)
                snapshotDAO.exportSnapshot(resourceName, snapshot) {
                  case Some(SnapshotDAO.SnapshotInfo(lastModified, stream)) =>
                    val basename = basenameFor(resourceName, lastModified)
                    val result =
                      using(new ThreadlessGZipCompressInputStream(stream, gzipBufferSize)) { inStream =>
                        blobStoreManager.multipartUpload(inStream, s"$basename.csv.gz")
                      }
                    if(result.isRight) {
                      stream.close()
                      log.info("Purging snapshot {} on dataset {}", snapshot, resourceName.underlying)
                      snapshotDAO.deleteSnapshot(resourceName, snapshot)
                      try {
                        blobStoreManager.deleteCopiesExceedingQuota(resourceName.underlying, Quota)
                      } catch {
                        case e: Exception =>
                          log.warn("Error deleting snapshots exceeding quota dataset {}; ignoring", resourceName.underlying: Any, e)
                      }
                    } else {
                      log.warn("Problem uploading snapshot {} on dataset {} to the blobstore; not deleting it",
                        snapshot, resourceName.underlying)
                      // the blobstore will have already logged the actual problem.
                      // abort processing this dataset
                      break()
                    }
                  case None =>
                    log.info("Reported snapshot missing; ignoring it (maybe we lost leadership?)")
                }
              }
            }
          } catch {
            case e: IOException =>
              log.warn("Error snapshotting dataset {}; ignoring", resourceName.underlying : Any, e)
          }
        }
      }
    }
  }
}
