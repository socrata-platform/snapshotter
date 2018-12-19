package com.socrata.snapshotter

import java.io.InputStream

import org.joda.time.DateTime

import scala.collection.immutable.SortedSet

trait SnapshotDAO {
  def datasetsWithSnapshots(): Set[ResourceName]
  def datasetSnapshots(dataset: ResourceName): SortedSet[Long]
  def exportSnapshot[T](dataset: ResourceName, snapshot: Long)(f: Option[SnapshotDAO.SnapshotInfo] => T): T
  def deleteSnapshot(dataset: ResourceName, snapshot: Long): Unit
}

object SnapshotDAO {
  case class SnapshotInfo(lastModified: DateTime, data: InputStream)
}
