package com.socrata.snapshotter

import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets._
import javax.servlet.http.HttpServletResponse
import javax.servlet.{WriteListener, ServletOutputStream}

import org.mockito.Mockito._

class ByteArrayServletOutputStream extends ServletOutputStream {
  val underlying: ByteArrayOutputStream = new ByteArrayOutputStream()
  override def isReady: Boolean = true

  override def setWriteListener(writeListener: WriteListener): Unit = {}

  override def write(b: Int): Unit = underlying.write(b)

  def getString: String = new String(underlying.toByteArray, UTF_8)

  val responseFor: HttpServletResponse = {
    val resp = mock(classOf[HttpServletResponse])
    when(resp.getOutputStream).thenReturn(this)
    resp
  }
}
