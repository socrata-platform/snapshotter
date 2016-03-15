package com.socrata.snapshotter

import java.io.Closeable

import scala.concurrent.duration.FiniteDuration
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.leader.LeaderLatch
import org.slf4j.LoggerFactory

class SodaWatcher(curatorFramework: CuratorFramework,
                  latchPath: String,
                  pause: FiniteDuration,
                  snapshotDAO: SnapshotDAO) extends Closeable {
  private val log = LoggerFactory.getLogger(classOf[SodaWatcher])
  private val latch = new LeaderLatch(curatorFramework, latchPath)
  private val pauseMS = pause.toMillis
  private var worker: Worker = null

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
        for {
          ds <- snapshotDAO.datasetsWithSnapshots()
          snapshot <- snapshotDAO.datasetSnapshots(ds)
        } {
          if(!latch.hasLeadership) { return }
          log.info("Purging snapshot {} on dataset {}", snapshot, ds)
          snapshotDAO.deleteSnapshot(ds, snapshot)
        }
      }
    }
  }
}
