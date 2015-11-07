package com.socrata.snapshotter

import java.io.InputStream

import com.amazonaws.event.{ProgressListener, ProgressEvent}
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.{ObjectMetadata, PutObjectRequest}
import com.amazonaws.services.s3.transfer.TransferManager
import com.amazonaws.services.s3.transfer.model.UploadResult

import com.rojoma.json.v3.interpolation._
import com.rojoma.json.v3.ast.JValue

import org.slf4j.LoggerFactory

object BlobManager {
  private lazy val s3client = new AmazonS3Client()
  private lazy val manager = new TransferManager(s3client)
  private val logger = LoggerFactory.getLogger(getClass)

  def shutdownManager(): Unit = {
    logger.info("Shutting down transfer manager.")
    manager.shutdownNow()
  }

  def upload(inStream: InputStream, keyName: String): UploadResult = {
    val req = new PutObjectRequest(SnapshotterConfig.awsBucketName, s"$keyName", inStream, new ObjectMetadata())
    logger.info(s"Sending put request to s3: $req")
    val upload = manager.upload(req)

    upload.addProgressListener(new SpecialListener(logger))

    val uploadResult = upload.waitForUploadResult()
    logger.info(s"uploadResult: $uploadResult")
    uploadResult
  }

  class SpecialListener(private val logger: org.slf4j.Logger) extends ProgressListener {
    def progressChanged(event: ProgressEvent): Unit = {
      if (event.getEventType == com.amazonaws.event.ProgressEventType.TRANSFER_COMPLETED_EVENT) {
        logger.info("Upload complete.")
      }
      logger.info(s"Transferred bytes: ${event.getBytesTransferred}")
    }
  }

}


