package com.socrata.snapshotter

import java.io.InputStream

import com.amazonaws.event.{ProgressListener, ProgressEvent}
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model._
import com.amazonaws.services.s3.transfer.TransferManager
import com.amazonaws.services.s3.transfer.model.UploadResult

import com.rojoma.json.v3.interpolation._
import com.rojoma.json.v3.ast.JValue
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

object BlobStoreManager {
  private lazy val s3client = new AmazonS3Client()
  private lazy val manager = new TransferManager(s3client)
  private val logger = LoggerFactory.getLogger(getClass)
  private val datasetIdLength = 9
  private val fileExtensionLength = 7 //.csv.gz

  def shutdownManager(): Unit = {
    logger.info("Shutting down transfer manager.")
    manager.shutdownNow()
  }

  def upload(inStream: InputStream, path: String): Either[JValue, UploadResult] = {
    try {
      val req = new PutObjectRequest(SnapshotterConfig.awsBucketName, s"$path", inStream, new ObjectMetadata())
      logger.debug(s"Sending put request to s3: $req")
      val upload = manager.upload(req)

      upload.addProgressListener(new LoggingListener(logger))

      val uploadResult = upload.waitForUploadResult()
      logger.debug(s"uploadResult: $uploadResult")
      Right(uploadResult)
    } catch {
      case exception: AmazonS3Exception => Left(
        json"""{ message: "Problem uploading to S3",
                     error: ${exception.toString},
                     "error code": ${exception.getErrorCode},
                     "error type": ${exception.getErrorType},
                     "error message": ${exception.getErrorMessage} }""")
    }

  }

  // TODO: account for possibility of truncated results (although not a problem in testing, as first 1000 results return)
  def listObjects(bucketName: String, path: String): JValue = {
    logger.info("Requesting a list...")
    s3client.listObjects(SnapshotterConfig.awsBucketName, path)
    val req = new ListObjectsRequest().withBucketName(bucketName).withPrefix(s"$path")
    val objectListing = s3client.listObjects(req)
    val objectSummaries: Seq[S3ObjectSummary] = objectListing.getObjectSummaries.asScala

    val snapshots: Seq[JValue] =
      objectSummaries.map( sum => {
        val key = sum.getKey
        logger.debug(s"Found key: ${key}")
        val (datasetId, date) = parseKey(key)
        json"""{ key:       ${sum.getKey},
                 datasetId: ${datasetId},
                 date:      ${date},
                 size:      ${sum.getSize} }"""})

    json"""{ "search prefix": $path, count: ${snapshots.length}, snapshots: $snapshots }"""
  }

  def parseKey(keyName: String): (String, String) = {
    val datasetId = keyName.slice(0,datasetIdLength)
    val timestamp = keyName.slice(datasetIdLength + 1, keyName.length - fileExtensionLength - 1 )
    (datasetId, timestamp)
  }

  class LoggingListener(private val logger: org.slf4j.Logger) extends ProgressListener {
    def progressChanged(event: ProgressEvent): Unit = {
      if (event.getEventType == com.amazonaws.event.ProgressEventType.TRANSFER_COMPLETED_EVENT) {
        logger.info("Upload complete.")
      }
      logger.info(s"Transferred bytes: ${event.getBytesTransferred}")
      logger.info(s"Progress event type: ${event.getEventType}")
    }
  }

}


