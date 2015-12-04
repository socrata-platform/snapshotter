package com.socrata.snapshotter

import java.io.{ByteArrayOutputStream, ByteArrayInputStream}

import com.socrata.snapshotter.StreamChunker.Chunk
import org.scalatest.mock.MockitoSugar
import org.scalatest.{MustMatchers, FunSuite}

import scala.util.Random

class StreamChunkerTest extends FunSuite with MustMatchers with MockitoSugar {

  // give input & buffer size in MB
  private def getChunker(inputSize: Int, bufferSize: Int): (StreamChunker, Array[Byte]) = {
    val inputBytes = new Array[Byte](inputSize)
    Random.nextBytes(inputBytes)
    val inStream = new SingleByteStream(inputBytes)
    (new StreamChunker(inStream, bufferSize), inputBytes)
  }

  private def countChunks(sc: StreamChunker): Int = {
    sc.foldLeft (0) ((sum, _) => sum + 1)
  }

  test("Returns divided input streams") {
    val (sc, _) = getChunker(21, 8)
    assert(countChunks(sc) == 3)
  }

  test("an input stream smaller than buffer size results in single input stream the same size as original input") {
    val (sc, _) = getChunker(3,8)
    val count = sc.chunks.foldLeft(0) { (count, chunk) =>
      assert(chunk.partNumber == 1)
      assert(chunk.size == 3)
      count + 1
    }
    assert(count == 1)
  }

  test("each chunked output stream is the same size as the buffer setting, when possible") {
    val (sc, _) = getChunker(8,2)
    sc.chunks.foreach { chunk => assert(chunk.size == 2) }
  }

  test("size field returned is accurate for the contents of the matching inputStream") {
    val (sc, _) = getChunker(4,2)
    sc.chunks.foreach { chunk =>
      // don't do this at home, kids
      val ba = Stream.continually(chunk.inputStream.read).takeWhile(_ != -1).map(_.toByte).toArray
      assert(ba.length == 2)
    }
  }

  test("input stream chunks combine to form the same original input data") {
    val (sc, inputBytes) = getChunker(20, 6)

    val outBytes = sc.chunks.foldLeft(new Array[Byte](0)) { (outBytes, chunk) =>
      outBytes ++ Stream.continually(chunk.inputStream.read).takeWhile(_ != -1).map(_.toByte)
    }
    assert(outBytes.sameElements(inputBytes))
  }

  test("you can read data from multiple chunks out of order") {
    val (sc, inputBytes) = getChunker(12, 6)
    val ba1 = new Array[Byte](6)
    val ba2 = new Array[Byte](6)
    val chunks = sc.chunks
    val chunk1 = chunks.next()
    val chunk2 = chunks.next()
    chunk2.inputStream.read(ba2)
    chunk1.inputStream.read(ba1)
    assert(inputBytes.sameElements(ba1 ++ ba2))
  }

  /* this class is used to test read_to_capacity.
  It simulates the fact that, which larger buffer sizes, read(buffer) doesn't always completely fill the buffer,
  even when there is more data available. */
  class SingleByteStream(val bytes: Array[Byte]) extends ByteArrayInputStream(bytes) {
    override def read(output: Array[Byte], offset: Int, len: Int): Int = {
      val byte = this.read()
      if (byte == -1) {
        byte
      } else {
        output(offset) = byte.toByte
        1
      }
    }
  }

}
