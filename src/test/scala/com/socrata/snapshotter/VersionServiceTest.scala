package com.socrata.snapshotter

import java.io.ByteArrayOutputStream
import javax.servlet.{WriteListener, ServletOutputStream}
import javax.servlet.http.HttpServletResponse
import java.nio.charset.StandardCharsets.UTF_8

import com.socrata.http.server.HttpRequest
import org.scalatest.FunSuite
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito.{verify, when}
import org.scalatest.MustMatchers


class VersionServiceTest extends FunSuite with MustMatchers with MockitoSugar {
  test("Endpoint returns a version") {
    val req = mock[HttpRequest]
    val os = new ByteArrayServletOutputStream
    val resp = os.responseFor

    VersionService.get(req)(resp)

    verify(resp).setStatus(200)
    verify(resp).setContentType("application/json; charset=UTF-8")

    os.getString.toLowerCase must include ("version")

  }
}
