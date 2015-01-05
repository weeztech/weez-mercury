package com.weez.mercury.common

import java.security.SecureRandom

import org.parboiled.common.Base64

object Util {

  class RandomIdGenerator(len: Int) {
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
}
