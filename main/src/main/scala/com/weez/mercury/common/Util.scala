package com.weez.mercury.common

import java.security.SecureRandom

import org.parboiled.common.Base64

import scala.util.matching.Regex

object Util {
  val devmode = com.typesafe.config.ConfigFactory.load().getBoolean("weez-mercury.devmode")

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

  def resolvePath(pathExp: String) = {
    val re = new Regex("^(~)|\\$([a-zA-Z0-9_]+)|\\$\\{([a-zA-Z0-9_]+)\\}")
    re.replaceAllIn(pathExp, m => {
      var exp: String = null
      var i = 1
      while (exp == null) {
        exp = m.group(i)
        i += 1
      }
      if (exp == "~") {
        exp = if (System.getProperty("os.name").startsWith("Windows")) "USERPROFILE" else "HOME"
      }
      exp = System.getenv(exp)
      val sep = java.io.File.separator
      exp = if (exp.endsWith(sep)) exp.substring(0, exp.length - 1) else exp
      if (sep != "/")
        exp.replace(sep, "/")
      else
        exp
    })
  }
}
