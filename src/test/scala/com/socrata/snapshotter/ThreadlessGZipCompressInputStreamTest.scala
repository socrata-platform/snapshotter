package com.socrata.snapshotter

import java.io.{EOFException, InputStream, ByteArrayOutputStream, ByteArrayInputStream}
import java.util.zip.GZIPInputStream

import org.apache.commons.io.IOUtils
import org.scalatest.prop.PropertyChecks
import org.scalatest.{MustMatchers, FunSuite}

import scala.annotation.tailrec

class ThreadlessGZipCompressInputStreamTest extends FunSuite with MustMatchers with PropertyChecks {
  final class ChunkedInputStream(var chunks: List[InputStream]) extends InputStream {
    @tailrec
    override def read(): Int =
      if(chunks.isEmpty) -1
      else chunks.head.read() match {
        case -1 =>
          chunks = chunks.tail
          read()
        case b =>
          b
      }

    @tailrec
    override def read(bs: Array[Byte], offset: Int, length: Int) =
      if(chunks.isEmpty) -1
      else chunks.head.read(bs, offset, length) match {
        case -1 =>
          chunks = chunks.tail
          read(bs, offset, length)
        case n =>
          n
      }
  }

  test("ThreadlessGZipCompressInputStream0 works") {
    val bufsiz0 = 1
    val input0: List[List[Byte]] = List(List(127))
    locally {
      val bufsiz = (bufsiz0 & 0xff) + 1
      val input = new ChunkedInputStream(input0.map(_.toArray).map(new ByteArrayInputStream(_)))

      val compressed = new ThreadlessGZipCompressInputStream(input, bufsiz)
      val baos = new ByteArrayOutputStream
      IOUtils.copy(compressed, baos)
      compressed.close()

      val gzis = new GZIPInputStream(new ByteArrayInputStream(baos.toByteArray))
      val target = input0.flatten.toArray
      val received = new Array[Byte](target.length)
      IOUtils.readFully(gzis, received)
      received must equal (target)
      gzis.read() must be (-1)
    }
  }

  test("ThreadlessGZipCompressInputStream works") {
    forAll { (bufsiz0: Int, input0: List[List[Byte]]) =>
      val bufsiz = (bufsiz0 & 0xfff) + 1
      val input = new ChunkedInputStream(input0.map(_.toArray).map(new ByteArrayInputStream(_)))

      val compressed = new ThreadlessGZipCompressInputStream(input, bufsiz)
      val baos = new ByteArrayOutputStream
      IOUtils.copy(compressed, baos)
      compressed.close()

      val gzis = new GZIPInputStream(new ByteArrayInputStream(baos.toByteArray))
      val target = input0.flatten.toArray
      val received = new Array[Byte](target.length)
      IOUtils.readFully(gzis, received)
      received must equal (target)
      gzis.read() must be (-1)
      // an [EOFException] must be thrownBy (gzis.read()) // because gzipinputstream only lets you see its EOF once
    }
  }
}
