package com.weez.mercury.common

object Util {

  class SecureIdGenerator(len: Int) {

    import org.parboiled.common.Base64
    import java.security.SecureRandom

    private var seed = 0
    private val base64 = Base64.rfc2045()
    private val secureRandom = {
      val sr = SecureRandom.getInstance("NativePRNG", "SUN")
      sr.setSeed(System.currentTimeMillis())
      sr
    }

    def newId: String = {
      val arr = new Array[Byte](len - 4)
      secureRandom.nextBytes(arr)
      seed += 1
      arr(0) = (seed >> 24).asInstanceOf[Byte]
      arr(1) = (seed >> 16).asInstanceOf[Byte]
      arr(2) = (seed >> 8).asInstanceOf[Byte]
      arr(3) = seed.asInstanceOf[Byte]
      base64.encodeToString(arr, false)
    }
  }

  def compareUInt8s(a: Array[Byte], b: Array[Byte]): Int = {
    compareUInt8s(a, 0, a.length, b, 0, b.length)
  }

  def compareUInt8s(a: Array[Byte], aStart: Int, aEnd: Int, b: Array[Byte], bStart: Int, bEnd: Int): Int = {
    val aLen = aEnd - aStart
    val bLen = bEnd - bStart
    val len = aLen.min(bLen)
    var c = 0
    var i = 0
    while (c == 0 && i < len) {
      c = (a(aStart + i) & 0xff) - (b(bStart + i) & 0xff)
      i += 1
    }
    if (c == 0) aLen - bLen else c
  }

  def concatUInt8s(parts: Array[Byte]*) = {
    val len = parts.foldLeft(0)((c, x) => c + x.length)
    val arr = new Array[Byte](len)
    var offset = 0
    parts foreach { x =>
      System.arraycopy(x, 0, arr, offset, x.length)
      offset += x.length
    }
    arr
  }

  def camelCase2seqStyle(name: String) = {
    import scala.util.matching.Regex
    new Regex("[A-Z]+").replaceAllIn(name, { m =>
      if (m.start == 0)
        m.matched.toLowerCase
      else
        "-" + m.matched.toLowerCase
    })
  }
}
