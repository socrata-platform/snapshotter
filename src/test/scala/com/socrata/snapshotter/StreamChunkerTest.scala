package com.socrata.snapshotter

import java.io.{ByteArrayOutputStream, ByteArrayInputStream}

import org.scalatest.mock.MockitoSugar
import org.scalatest.{MustMatchers, FunSuite}

class StreamChunkerTest extends FunSuite with MustMatchers with MockitoSugar {

  // give input & buffer size in MB
  private def getChunker(inputSize: Int, bufferSize: Int): StreamChunker = {
    val bais = new ByteArrayInputStream(new Array[Byte](inMB(inputSize)))
    new StreamChunker(bais, inMB(bufferSize))
  }

  private def countChunks(sc: StreamChunker): Int = {
    sc.foldLeft (0) ((sum, _) => sum + 1)
  }

  private def inMB(num: Int): Int = {
    num * 1024 * 1024
  }

  test("Returns divided input streams") {
    assert(countChunks(getChunker(21, 8)) == 3)
  }

  test("an input stream smaller than buffer size results in single input stream the same size as original input") {
    val sc = getChunker(3,8)
    val count = sc.chunks.foldLeft(0) { (count, chunk) =>
      assert(chunk.partNumber == 1)
      assert(chunk.size == inMB(3))
      count + 1
    }
    assert(count == 1)
  }

  test("each chunked output stream is the same size as the buffer setting, when possible") {
    val sc = getChunker(8,2)
    sc.chunks.foreach { chunk => assert(chunk.size == inMB(2)) }
  }

  test("size field returned is accurate for the contents of the matching inputStream") {
    val sc = getChunker(4,2)
    sc.chunks.foreach { chunk =>
      // don't do this at home, kids
      val ba = Stream.continually(chunk.inputStream.read).takeWhile(_ != -1).map(_.toByte).toArray
      assert(ba.length == inMB(2))
    }
  }

  test("input stream chunks combine to form the same original input data") {
    val input = "Ordering test for a string chunked into input streams"
    val inBytes = input.getBytes
    val bais = new ByteArrayInputStream(inBytes)
    val sc = new StreamChunker(bais, inBytes.length)

    val outBytes = sc.chunks.foldLeft(new Array[Byte](0)) { (outBytes, chunk) =>
      outBytes ++ Stream.continually(chunk.inputStream.read).takeWhile(_ != -1).map(_.toByte)
    }
    val output = new String(outBytes)
    assert(output == input)
  }
}
