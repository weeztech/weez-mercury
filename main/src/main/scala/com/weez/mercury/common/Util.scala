package com.weez.mercury.common

object Util {
  val devmode = com.typesafe.config.ConfigFactory.load().getBoolean("weez-mercury.devmode")

  class RandomIdGenerator(len: Int) {

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

  def resolvePath(pathExp: String) = {
    import scala.util.matching.Regex
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
        exp.replace("/", sep)
      else
        exp
    })
  }

  def deleteDirectory(path: String): Unit = {
    import java.nio.file._
    val p = Paths.get(path)
    if (Files.exists(p, LinkOption.NOFOLLOW_LINKS)) {
      if (Files.isSymbolicLink(p)) {
        Files.delete(p)
      } else if (Files.isDirectory(p)) {
        val dirs = scala.collection.mutable.Stack[Path]()
        val emptyDirs = scala.collection.mutable.Stack[Path]()
        dirs.push(p)
        while (dirs.nonEmpty) {
          val p = dirs.pop
          emptyDirs.push(p)
          val dir = Files.newDirectoryStream(p)
          val toDelete = scala.collection.mutable.ListBuffer[Path]()
          try {
            val itor = dir.iterator()
            while (itor.hasNext) {
              val p = itor.next()
              if (Files.isDirectory(p, LinkOption.NOFOLLOW_LINKS))
                dirs.push(p)
              else
                toDelete.append(p)
            }
            toDelete foreach Files.delete
            toDelete.clear()
          } finally {
            dir.close()
          }
        }
        while (emptyDirs.nonEmpty) {
          Files.delete(emptyDirs.pop)
        }
      } else
        throw new Exception("not a directory")
    }
  }

  def compareUInt8s(a: Array[Byte], b: Array[Byte]): Int = {
    val l = a.length max b.length
    var i, c = 0
    while (c == 0 && i < l) {
      c = (a(i) & 0xFF) - (b(i) & 0xFF)
      i += 1
    }
    if (c != 0) c else a.length - b.length
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
