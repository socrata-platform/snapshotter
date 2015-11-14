package com.socrata.snapshotter

import java.io.{ByteArrayInputStream, InputStream}

import org.slf4j.LoggerFactory

import StreamChunker._

class StreamChunker(inStream: InputStream, bufferSize: Int) extends Iterator[(ByteArrayInputStream, Int)] {
  private var chunkSize: Option[Int] = None
  val buffer = new Array[Byte](bufferSize)
  private val logger = LoggerFactory.getLogger(getClass)

  def hasNext = {
    // if the last thing has been read out
    if (chunkSize.isEmpty) {
      val sizeRead = inStream.read(buffer)
      logger.debug("Claiming to have read {} bytes of data into the buffer", sizeRead)
      chunkSize = if (sizeRead != -1) Some(sizeRead) else None
    }
    // for sure hasNext, because the chunk is there & waiting
    chunkSize.isDefined
  }

  def next(): (ByteArrayInputStream, Int) = {
    if (!hasNext) {
      throw new IllegalStateException("There is no next chunk available or hasNext was not called.")
    }
    val returnSize = chunkSize.get
    logger.debug("Returning next chunk stream, size: {}", returnSize)
    logger.debug("This chunk is greater than 5 MB: {}", returnSize > (5 * 1024 * 1024))
    val chunkStream = new ByteArrayInputStream(buffer, 0, chunkSize.get)
    chunkSize = None
    (chunkStream, returnSize)
  }

  def chunks: Iterator[Chunk] = {
    val tails: Iterator[Stream[(ByteArrayInputStream, Int)]] = this.toStream.tails
    val notEmpty = tails.filterNot(_.isEmpty)

    val needsIndex = notEmpty.map { s =>
      val (is, size) = s.head
      val last = s.tail.isEmpty
      (is, size, last)
    }

    needsIndex.zipWithIndex.map { case ((is, size, last), index) =>
      new Chunk(is, size, last, index + 1)
    }
  }
}

object StreamChunker {
  case class Chunk(inputStream: InputStream, size: Int, isLast: Boolean, partNumber: Int)
}
