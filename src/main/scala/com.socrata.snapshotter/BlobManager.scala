package com.socrata.snapshotter

import java.io.InputStream

import com.amazonaws.event.{ProgressListener, ProgressEvent}
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.{AmazonS3Exception, ObjectMetadata, PutObjectRequest}
import com.amazonaws.services.s3.transfer.TransferManager
import com.amazonaws.services.s3.transfer.model.UploadResult

import com.rojoma.json.v3.interpolation._
import com.rojoma.json.v3.ast.JValue

import org.slf4j.LoggerFactory

object BlobStoreManager {
  private lazy val s3client = new AmazonS3Client()
  private lazy val manager = new TransferManager(s3client)
  private val logger = LoggerFactory.getLogger(getClass)

  def shutdownManager(): Unit = {
    logger.info("Shutting down transfer manager.")
    manager.shutdownNow()
  }

  def upload(inStream: InputStream, path: String): Either[JValue, UploadResult] = {
    try {
      val req = new PutObjectRequest(SnapshotterConfig.awsBucketName, s"$path", inStream, new ObjectMetadata())
      logger.info(s"Sending put request to s3: $req")
      val upload = manager.upload(req)

      upload.addProgressListener(new LoggingListener(logger))

      val uploadResult = upload.waitForUploadResult()
      logger.info(s"uploadResult: $uploadResult")
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

  class LoggingListener(private val logger: org.slf4j.Logger) extends ProgressListener {
    def progressChanged(event: ProgressEvent): Unit = {
      if (event.getEventType == com.amazonaws.event.ProgressEventType.TRANSFER_COMPLETED_EVENT) {
        logger.info("Upload complete.")
      }
      logger.info(s"Transferred bytes: ${event.getBytesTransferred}")
    }
  }

}


