package com.socrata.snapshotter

import java.io.InputStream

import com.amazonaws.event.{ProgressListener, ProgressEvent}
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.{ObjectMetadata, PutObjectRequest, AmazonS3Exception}
import com.amazonaws.services.s3.transfer.TransferManager
import com.amazonaws.services.s3.transfer.model.UploadResult

import com.rojoma.json.v3.interpolation._
import com.rojoma.json.v3.ast.JValue

import org.slf4j.LoggerFactory

object BlobManager {
  private lazy val manager = new TransferManager()
  private lazy val s3client = new AmazonS3Client()
  private val logger = LoggerFactory.getLogger(getClass)

  /* It would be great if this would proceed under the covers and shut itself down when done, but then how to check the result
  */
  def upload(inStream: InputStream, keyName: String): UploadResult = {
    val req = new PutObjectRequest(SnapshotterConfig.awsBucketName, s"keyName", inStream, new ObjectMetadata())
    logger.info(s"Sending put request to s3: $req")
    val upload = manager.upload(req)

    upload.addProgressListener(new ProgressListener() {
      override def progressChanged(event: ProgressEvent): Unit =
        logger.info(s"Transferred bytes: ${event.getBytesTransferred}")
    })

    val uploadResult = upload.waitForUploadResult()
    manager.shutdownNow()
    logger.info(s"uploadResult: $uploadResult")
    uploadResult
  }

}
