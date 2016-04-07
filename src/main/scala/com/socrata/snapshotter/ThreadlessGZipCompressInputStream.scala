package com.socrata.snapshotter

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, InputStream}
import java.util.zip.GZIPOutputStream

import org.apache.commons.io.IOUtils

class ThreadlessGZipCompressInputStream(underlying: InputStream, bufSize: Int = 4096) extends InputStream {
  private var closed = false
  private var eofSeen = false
  private val buf = new Array[Byte](bufSize)
  private var bais = new ByteArrayInputStream(new Array[Byte](0))
  private val baos = new ByteArrayOutputStream
  private val gz = new GZIPOutputStream(baos)

  override def close(): Unit = synchronized {
    if(!closed) {
      closed = true
      try {
        if(!eofSeen) gz.close()
      } finally {
        underlying.close()
      }
    }
  }

  private def refill(): Unit = {
    while(baos.size == 0 && !eofSeen) {
      val n = IOUtils.read(underlying, buf)
      gz.write(buf, 0, n)
      if(n < buf.length) {
        eofSeen = true
        gz.close()
      }
    }
  }

  private def readPrelude(): Unit = {
    if(bais.available == 0) {
      if(baos.size == 0) {
        refill()
      }
      bais = new ByteArrayInputStream(baos.toByteArray)
      baos.reset()
    }
  }

  override def read(): Int = synchronized {
    readPrelude()
    bais.read()
  }

  override def read(bs: Array[Byte], offset: Int, length: Int): Int = synchronized {
    readPrelude()
    bais.read(bs, offset, length)
  }
}
