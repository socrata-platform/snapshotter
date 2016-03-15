package com.socrata.snapshotter

import com.rojoma.json.v3.codec.JsonDecode
import com.socrata.curator.CuratedServiceClient
import scala.collection.immutable.SortedSet
import com.socrata.http.client.{Response, HttpClient, RequestBuilder}

class SnapshotDAOImpl(sfClient: CuratedServiceClient) extends SnapshotDAO {
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

  def datasetsWithSnapshots(): Set[String] =
    get[Set[String]]("snapshot").getOrElse(Set.empty)

  def datasetSnapshots(dataset: String): SortedSet[Long] =
    get[SortedSet[Long]]("snapshot", dataset).getOrElse(SortedSet.empty)

  def deleteSnapshot(dataset: String, snapshot: Long): Unit =
    delete("snapshot", dataset, snapshot.toString)
}
