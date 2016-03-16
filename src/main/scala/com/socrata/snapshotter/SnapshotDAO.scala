package com.socrata.snapshotter

import scala.collection.immutable.SortedSet

trait SnapshotDAO {
  def datasetsWithSnapshots(): Set[String]
  def datasetSnapshots(dataset: String): SortedSet[Long]
  def deleteSnapshot(dataset: String, snapshot: Long): Unit
}
