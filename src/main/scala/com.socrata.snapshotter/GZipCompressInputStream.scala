package com.socrata.snapshotter

import java.io.{PipedOutputStream, PipedInputStream, IOException, InputStream}
import java.util.zip.GZIPOutputStream

import com.rojoma.simplearm.v2.using
import org.slf4j.LoggerFactory

class GZipCompressInputStream(val underlying: InputStream, pipeBufferSize: Int) extends InputStream {
  private val worker: Worker = new Worker(underlying, pipeBufferSize)
  private val logger = LoggerFactory.getLogger(getClass)

  worker.start()

  override def read(): Int = worker.read()

  override def read(bytes: Array[Byte], off: Int, len: Int): Int = {
    worker.read(bytes, off, len)
  }

  override def close(): Unit = {
    worker.shutdown()
    underlying.close()
  }

  private class Worker(val in: InputStream, val pipeBufferSize: Int) extends Thread {
    setDaemon(true)
    setName("Compression thread")
    val ReadFinished = -1
    val compressed = new PipedInputStream(pipeBufferSize)
    val pipe = new PipedOutputStream
    var pendingException: Option[IOException] = None

    pipe.connect(compressed)

    // in -> compressor -> pipe -> compressed
    override def run(): Unit = {
      try {
        using(new GZIPOutputStream(pipe, pipeBufferSize)) { compressor =>
          val buffer: Array[Byte] = new Array(pipeBufferSize)
          var count: Int = in.read(buffer)

          while (count != ReadFinished) {
            compressor.write(buffer, 0, count)
            count = in.read(buffer)
          }
        }
      } catch {
        case e: IOException => pendingException = Some(e)
      }
    }

    def read(): Int = {
      val res = compressed.read()
      if (res == ReadFinished && pendingException.isDefined) throw pendingException.get
      res
    }

    def read(bytes: Array[Byte], baseOff: Int, len: Int): Int = {
      logger.info(s"Read called with length of: $len/${bytes.length}")

      var byte = 0
      var i = baseOff

      while (i < len && byte != ReadFinished) {
        byte = read()
        if (byte != ReadFinished) {
          bytes(i) = byte.toByte
          i += 1
        }
      }

      logger.info(s"Returning read after having read: ${i - baseOff}")
      if (byte == ReadFinished && i == baseOff) ReadFinished else i - baseOff
    }

    def shutdown(): Unit = {
      compressed.close()
      in.close()
    }
  }
}


