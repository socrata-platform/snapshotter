package com.socrata.snapshotter

import java.io.{ByteArrayInputStream, InputStream}

import org.slf4j.LoggerFactory

import StreamChunker._

class StreamChunker(inStream: InputStream, bufferSize: Int) extends Iterator[(ByteArrayInputStream, Int)] {

  private val ReadFinished = -1
  private val logger = LoggerFactory.getLogger(getClass)
  private var chunkSize: Option[Int] = None
  val buffer = new Array[Byte](bufferSize)

  def hasNext: Boolean = {
    // if the last thing has been read out
    if (chunkSize.isEmpty) {
      val sizeRead = readToCapacity(inStream, buffer)
      chunkSize = if (sizeRead != ReadFinished) Some(sizeRead) else None
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

  /** Get an iterator of nicely packaged chunk objects, each containing: inputStream, streamSize, and partNumber */
  def chunks: Iterator[Chunk] = {
    this.zipWithIndex.map { case ((stream, streamSize), index) =>
      logger.debug("Creating chunk#{} with size of {}", index + 1, streamSize)
      new Chunk(stream, streamSize, index + 1)
    }
  }
  
  // Read from the input stream until the buffer is completely filled (or stream is finished)
  private def readToCapacity(input: InputStream, buffer: Array[Byte]): Int = {

    def loop(bytes: Array[Byte], loopOffest: Int, len: Int): Int = {
      logger.debug("Looping inside read with offset: {}, length limit: {}", loopOffest, len)
      val bytesRead = input.read(bytes, loopOffest, len)
      val newOffset = loopOffest + bytesRead
      val newLen = len - bytesRead
      val delta = if (bytesRead != ReadFinished && newLen != 0) loop(bytes, newOffset, newLen) else 0
      if (bytesRead > 0) bytesRead + delta else delta
    }

    logger.debug("readToCapacity called, byte array length: {}", buffer.length)
    val bytesRead = input.read(buffer)

    // if the very first read attempt returns -1, we return -1. otherwise we need to return the bytesRead
    if (bytesRead == ReadFinished) {
      logger.debug("Returning readToCapacity with -1, finished reading input stream")
      ReadFinished
    } else {
      val newLen = buffer.length - bytesRead
      val loopTotal = if (bytesRead != ReadFinished && newLen != 0) loop(buffer, bytesRead, newLen) else 0
      logger.debug("Returning readToCapacity with total read: {}", loopTotal + bytesRead)
      loopTotal + bytesRead
    }
  }
}

object StreamChunker {
  case class Chunk(inputStream: InputStream, size: Int, partNumber: Int)
}
