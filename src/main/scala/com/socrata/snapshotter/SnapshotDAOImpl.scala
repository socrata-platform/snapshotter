package com.socrata.snapshotter

import com.rojoma.json.v3.codec.JsonDecode
import com.rojoma.simplearm.v2.ResourceScope
import com.socrata.curator.CuratedServiceClient
import com.socrata.http.common.util.HttpUtils
import com.socrata.snapshotter.SnapshotDAO.SnapshotInfo
import org.joda.time.DateTime
import scala.collection.immutable.SortedSet
import com.socrata.http.client.Response

class SnapshotDAOImpl(sfClient: CuratedServiceClient) extends SnapshotDAO {
  val log = org.slf4j.LoggerFactory.getLogger(classOf[SnapshotDAOImpl])
  val stdTimeout = 30000 // 30s

  private def get[T : JsonDecode](s: String*): Option[T] = {
    def handler(r: Response) =
      r.resultCode match {
        case 200 =>
          r.value[T]() match {
            case Right(result) =>
              Some(result)
            case Left(err) =>
              throw new Exception("Unable to parse GET for " + s.mkString("/","/","") + ": " + err.english)
          }
        case 404 =>
          None
        case other =>
          throw new Exception("Unexpected result code for GET of " + s.mkString("/","/","") + ": " + other)
      }
    sfClient.execute(_.p(s: _*).timeoutMS(stdTimeout).get, handler)
  }

  private def delete(s: String*): Boolean = {
    def handler(r: Response) =
      r.resultCode match {
        case 200 =>
          true
        case 404 =>
          false
        case other =>
          throw new Exception("Unexpected result code for DELETE of " + s.mkString("/","/","") + ": " + other)
      }
    sfClient.execute(_.p(s:_*).timeoutMS(stdTimeout).delete, handler)
  }

  def datasetsWithSnapshots(): Set[ResourceName] =
    get[Set[String]]("snapshot").getOrElse(Set.empty).map(ResourceName)

  def datasetSnapshots(dataset: ResourceName): SortedSet[Long] =
    get[SortedSet[Long]]("snapshot", dataset.underlying).getOrElse(SortedSet.empty)

  def deleteSnapshot(dataset: ResourceName, snapshot: Long): Unit =
    delete("snapshot", dataset.underlying, snapshot.toString)

  private def lastModified(r: Response): Option[DateTime] =
    r.headers("last-modified").headOption.map(HttpUtils.parseHttpDate)

  override def exportSnapshot[T](dataset: ResourceName, snapshot: Long)(f: Option[SnapshotInfo] => T): T = {
    def handler(r: Response) =
      r.resultCode match {
        case 200 =>
          lastModified(r) match {
            case Some(lm) =>
              f(Some(SnapshotInfo(lm, r.inputStream())))
            case None =>
              log.warn("No last-modified for dataset {} snapshot {}?", dataset.underlying, snapshot)
              f(None)
          }
        case 404 =>
          f(None)
      }
    sfClient.execute(_.p("snapshot", dataset.underlying, snapshot.toString).get, handler)
  }
}
