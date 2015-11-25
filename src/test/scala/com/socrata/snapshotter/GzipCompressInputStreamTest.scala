package com.socrata.snapshotter

import java.io.ByteArrayInputStream
import java.util.zip.GZIPInputStream

import org.scalatest.FunSuite

import scala.util.Random

class GzipCompressInputStreamTest extends FunSuite {

  lazy val inputString = "Every time. Britannia is here. Yeah, ha ha ha ha}. Word and snow-white tan. Far above the street. What can beat them on a junkie. Eh, eh how could not mess. Walk out in synthesis and meet us. Sadden glissando strings. He was awful nice. Are talking this creature fair. Watching some good with. Ain't got five years, my eyes. Woo! Okay. Like to be us locked up the last, the dance with me, don't know? Should I say, why didn't I still. Send him right now. Throwing darts in my wall by the feet of the world is that it. Lady stardust sang all. Smiling and that you so I will be late - to come and looking so how could spit in Heaven's high. Check ignition and your shame was all the taste at all together. things done. No, beep-beep.."
  lazy val inputBytes = inputString.getBytes

  def getZippedStream(inBytes: Array[Byte], bufferSize: Int): GZipCompressInputStream = {
    val bais = new ByteArrayInputStream(inBytes)
    new GZipCompressInputStream(bais,bufferSize)
  }

  test("total output size should be smaller than inputsize") {
    val inputSize = inputBytes.length
    val zippedStream = getZippedStream(inputBytes, 70)
    val ba = Stream.continually(zippedStream.read).takeWhile(_ != -1).map(_.toByte).toArray
    assert(ba.length < inputSize, "Compressed size was not less than uncompressed size")
  }

   test("output format should be gzip compatible") {
     val zippedStream = getZippedStream(inputBytes, 70)
     try {
       val unzippedStream = new GZIPInputStream(zippedStream)
     } catch{
       case _: java.util.zip.ZipException =>
         assert(false, "Expected a Gzip formatted input stream")
     }
     assert(true)
   }

  test("once unzipped, output contents should match input contents") {
    val zippedStream = getZippedStream(inputBytes, 70)
    val unzippedStream = new GZIPInputStream(zippedStream)
    val ba = Stream.continually(unzippedStream.read()).takeWhile(_ != -1).map(_.toByte).toArray
    assert(ba.sameElements(inputBytes))
  }

}
