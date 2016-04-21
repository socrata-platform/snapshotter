package com.socrata.snapshotter

import java.io.{Closeable, InputStream}
import java.util.Date

import scala.collection.JavaConverters._

import com.amazonaws.event.{ProgressListener, ProgressEvent}
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model._
import com.amazonaws.services.s3.transfer.TransferManager
import com.amazonaws.services.s3.transfer.model.UploadResult
import com.rojoma.json.v3.interpolation._
import com.rojoma.json.v3.ast.JValue
import com.rojoma.simplearm.v2._
import org.slf4j.LoggerFactory

class BlobStoreManager(bucketName: String, uploadPartSize: Int) extends Closeable {
  private lazy val s3client = new AmazonS3Client()
  private lazy val manager = new TransferManager(s3client)
  private val logger = LoggerFactory.getLogger(getClass)

  def close(): Unit = {
    logger.debug("Shutting down transfer manager.")
    manager.shutdownNow() // also shuts down the s3 client
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
      val md = new ObjectMetadata()
      md.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION)
      val req = new PutObjectRequest(bucketName, path, inStream, md)
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
        val md = new ObjectMetadata()
        md.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION)
        val req = new InitiateMultipartUploadRequest(bucketName, path)
        req.setObjectMetadata(md)
        s3client.initiateMultipartUpload(req)
      }
      val uploadId = initResult.getUploadId
      val partReqs = createRequests(initResult.getUploadId, path, inStream)
      // make sure to send s3 a mutable java list
      val uploadPartTags = new java.util.ArrayList(sendRequests(partReqs).asJava)

      logger.debug("Requesting to complete multipart upload")
      Right(retrying {
              s3client.completeMultipartUpload(
                new CompleteMultipartUploadRequest(bucketName, path, uploadId, uploadPartTags))
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

  def fetch(path: String, resourceScope: ResourceScope): Option[S3Object] =
    try {
      Some(resourceScope.open(retrying(s3client.getObject(bucketName, path))))
    } catch {
      case e: AmazonS3Exception if e.getStatusCode == 404 =>
        None
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
      withBucketName(bucketName).
      withUploadId(uploadId).
      withKey(path).
      withInputStream(chunk.inputStream).
      withPartSize(chunk.size).
      withPartNumber(chunk.partNumber)
    }
  }

  // TODO: account for possibility of truncated results (not a problem in testing, as first 1000 results return)
  def listObjects(path: String): JValue = {
    logger.debug("Requesting a list...")
    val req = new ListObjectsRequest().withBucketName(bucketName).withPrefix(path)
    val objectListing = retrying(s3client.listObjects(req))
    val objectSummaries: Seq[S3ObjectSummary] = objectListing.getObjectSummaries.asScala

    val snapshots: Seq[JValue] =
      objectSummaries.collect {
        case ParseKey(key, size, datasetId, timestamp) =>
          logger.debug(s"Found key: ${key}")
          json"""{ datasetId: ${datasetId.underlying},
                   date:      ${timestamp},
                   size:      ${size} }"""
      }

    json"""{ "search prefix": $path, count: ${snapshots.length}, snapshots: $snapshots }"""
  }

  def abortMultiPartUploads(): Unit = {
    manager.abortMultipartUploads(bucketName, new Date())
  }

  object ParseKey {
    // Our filenames are "resourcename:timestamp"; ":" is not a legal character in resource names
    val Pattern = """(.*):(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.?\d*Z)\.csv\.gz""".r
    def unapply(summary: S3ObjectSummary): Option[(String, Long, ResourceName, String)] =
      summary.getKey match {
        case Pattern(datasetId, timestamp) =>
          Some((summary.getKey, summary.getSize, ResourceName(datasetId), timestamp))
        case _ =>
          None
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


