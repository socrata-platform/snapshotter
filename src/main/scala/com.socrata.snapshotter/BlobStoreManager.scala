package com.socrata.snapshotter

import java.io.InputStream
import java.util.Date

import scala.collection.JavaConverters._

import com.amazonaws.event.{ProgressListener, ProgressEvent}
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model._
import com.amazonaws.services.s3.transfer.TransferManager
import com.amazonaws.services.s3.transfer.model.UploadResult
import com.rojoma.json.v3.interpolation._
import com.rojoma.json.v3.ast.JValue
import org.slf4j.LoggerFactory

object BlobStoreManager {
  private lazy val s3client = new AmazonS3Client()
  private lazy val manager = new TransferManager(s3client)
  private val logger = LoggerFactory.getLogger(getClass)
  private val uploadPartSize = SnapshotterConfig.uploadPartSize

  def shutdownManager(): Unit = {
    logger.debug("Shutting down transfer manager.")
    manager.shutdownNow()
  }

  private def retrying[T](op: =>T): T = {
    val retryLimit = 4
    def loop(retryCount: Int, retryDelay: Int): T = {
      try {
        op
      } catch {
        case e: AmazonS3Exception if e.getStatusCode == 503 || e.getStatusCode == 500 =>
          // sometimes AWS returns 503s or 500s for perfectly good requests; we'll
          // treat them as transient failures and retry the opeartion
          if(retryCount < retryLimit) {
            Thread.sleep(retryDelay)
            loop(retryCount + 1, retryDelay * 2)
          } else {
            throw e
          }
      }
    }
    loop(0, 100) // max total sleep time = 3.1s
  }

  def upload(inStream: InputStream, path: String): Either[JValue, UploadResult] = {
    try {
      val req = new PutObjectRequest(SnapshotterConfig.awsBucketName, path, inStream, new ObjectMetadata())
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

  def multipartUpload(inStream: InputStream, path: String): Either[JValue, CompleteMultipartUploadResult] = {
    logger.debug(s"Upload part size set at: $uploadPartSize")
    try {
      logger.debug("Initiate multipart upload")
      val initResult = retrying {
          s3client.initiateMultipartUpload(
            new InitiateMultipartUploadRequest(SnapshotterConfig.awsBucketName, path))
        }
      val uploadId = initResult.getUploadId
      val partReqs = createRequests(initResult.getUploadId, path, inStream)
      // make sure to send s3 a mutable java list
      val uploadPartTags = new java.util.ArrayList(sendRequests(partReqs).asJava)

      logger.debug("Requesting to complete multipart upload")
      Right(retrying {
              s3client.completeMultipartUpload(
                new CompleteMultipartUploadRequest(SnapshotterConfig.awsBucketName, path, uploadId, uploadPartTags))
            })
    } catch {
      case exception: AmazonS3Exception =>
        val msg =
          json"""{ message: "Problem uploading to S3",
                       error: ${exception.toString},
                       "error code": ${exception.getErrorCode},
                       "error type": ${exception.getErrorType},
                       "error message": ${exception.getErrorMessage} }"""
        logger.warn(msg.toString())
        Left(msg)
    }
  }

  def sendRequests(partReqs: Iterator[UploadPartRequest]): List[PartETag] = {
    partReqs.map { req =>
      logger.debug(s"Requesting to upload part {}", req.getPartNumber)
      val partResp = retrying(s3client.uploadPart(req))
      partResp.getPartETag
    }.toList
  }

  def createRequests(uploadId: String, path: String, inStream: InputStream): Iterator[UploadPartRequest] = {
    logger.debug("Create stream chunker")
    val chunker = new StreamChunker(inStream, uploadPartSize)

    chunker.chunks.map { chunk => new UploadPartRequest().
      withBucketName(SnapshotterConfig.awsBucketName).
      withUploadId(uploadId).
      withKey(path).
      withInputStream(chunk.inputStream).
      withPartSize(chunk.size).
      withPartNumber(chunk.partNumber)
    }
  }

  // TODO: account for possibility of truncated results (not a problem in testing, as first 1000 results return)
  def listObjects(bucketName: String, path: String): JValue = {
    logger.debug("Requesting a list...")
    s3client.listObjects(SnapshotterConfig.awsBucketName, path)
    val req = new ListObjectsRequest().withBucketName(bucketName).withPrefix(s"$path")
    val objectListing = s3client.listObjects(req)
    val req = new ListObjectsRequest().withBucketName(bucketName).withPrefix(s"$path/").withDelimiter("/")
    val objectListing = retrying(s3client.listObjects(req))
    val objectSummaries: Seq[S3ObjectSummary] = objectListing.getObjectSummaries.asScala

    val snapshots: Seq[JValue] =
      objectSummaries.map( sum => {
        val key = sum.getKey
        logger.debug(s"Found key: ${key}")
        val (datasetId, timestamp) = parseKey(key)
        json"""{ key:       ${sum.getKey},
                 datasetId: ${datasetId},
                 date:      ${timestamp},
                 size:      ${sum.getSize} }"""})

    json"""{ "search prefix": $path, count: ${snapshots.length}, snapshots: $snapshots }"""
  }

  def abortMultiPartUploads(): Unit = {
    manager.abortMultipartUploads(SnapshotterConfig.awsBucketName, new Date())
  }

  def parseKey(keyName: String): (String, String) = {
    val pattern = "(^....-....)-(\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.?\\d*Z)\\.csv\\.gz".r
    try {
      val pattern(datasetId, timestamp) = keyName
      (datasetId, timestamp)
    } catch {
      case e:MatchError => {
        ("Unable to parse datasetId from key name", "Unable to parse snapshot date from key name")
      }
    }

  }

  class LoggingListener(private val logger: org.slf4j.Logger) extends ProgressListener {
    def progressChanged(event: ProgressEvent): Unit = {
      if (event.getEventType == com.amazonaws.event.ProgressEventType.TRANSFER_COMPLETED_EVENT) {
        logger.info("Upload complete.")
      }
      logger.debug(s"Transferred bytes: ${event.getBytesTransferred}")
      logger.debug(s"Progress event type: ${event.getEventType}")
    }
  }

}


