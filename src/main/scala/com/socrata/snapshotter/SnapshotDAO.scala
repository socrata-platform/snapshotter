package com.socrata.snapshotter

import java.io.InputStream

import com.rojoma.simplearm.v2.ResourceScope
import org.joda.time.DateTime

import scala.collection.immutable.SortedSet

trait SnapshotDAO {
  def datasetsWithSnapshots(): Set[String]
  def datasetSnapshots(dataset: String): SortedSet[Long]
  def exportSnapshot[T](dataset: String, snapshot: Long)(f: Option[SnapshotDAO.SnapshotInfo] => T): T
  def deleteSnapshot(dataset: String, snapshot: Long): Unit
}

object SnapshotDAO {
  case class SnapshotInfo(lastModified: DateTime, data: InputStream)
}
