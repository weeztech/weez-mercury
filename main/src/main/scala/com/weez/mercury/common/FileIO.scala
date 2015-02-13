package com.weez.mercury.common

import java.nio.file.{Path => JPath, Paths => JPaths, Files => JFiles, FileSystem => JFileSystem}

object FileIO {
  val seperator = "/"

  def pathOf(p: String) = make(concat(Nil, p), p.startsWith(seperator))

  def pathOfExp(p: String) = pathOf(resolvePathExp(p))

  def resolvePathExp(pathExp: String) = {
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
    import java.nio.file.LinkOption
    val p = JPaths.get(path)
    if (JFiles.exists(p, LinkOption.NOFOLLOW_LINKS)) {
      if (JFiles.isSymbolicLink(p)) {
        JFiles.delete(p)
      } else if (JFiles.isDirectory(p)) {
        val dirs = scala.collection.mutable.Stack[JPath]()
        val emptyDirs = scala.collection.mutable.Stack[JPath]()
        dirs.push(p)
        while (dirs.nonEmpty) {
          val p = dirs.pop()
          emptyDirs.push(p)
          val dir = JFiles.newDirectoryStream(p)
          val toDelete = scala.collection.mutable.ListBuffer[JPath]()
          try {
            val itor = dir.iterator()
            while (itor.hasNext) {
              val p = itor.next()
              if (JFiles.isDirectory(p, LinkOption.NOFOLLOW_LINKS))
                dirs.push(p)
              else
                toDelete.append(p)
            }
            toDelete foreach JFiles.delete
            toDelete.clear()
          } finally {
            dir.close()
          }
        }
        while (emptyDirs.nonEmpty)
          JFiles.delete(emptyDirs.pop())
      } else
        throw new Exception("not a directory")
    }
  }

  final class Path private[FileIO](private[Path] val parts: List[String], val isAbsolute: Boolean) {
    private lazy val jpath = JPaths.get(java.io.File.separator)

    def /(path: Path): Path = make(concat(parts, path.parts), isAbsolute)

    def /(path: String): Path = make(concat(parts, path), isAbsolute)

    def sibling(path: Path): Path = {
      require(parts.nonEmpty)
      make(concat(parts.tail, path.parts), isAbsolute)
    }

    def sibling(path: String): Path = {
      require(parts.nonEmpty)
      make(concat(parts.tail, path), isAbsolute)
    }

    def relative(path: Path): Path = make(rel(parts, path.parts), isAbsolute)

    def relative(path: String): Path = relative(pathOfExp(path))

    def in(path: Path): Boolean = parts.endsWith(path.parts)

    def in(path: String): Boolean = this in pathOfExp(path)

    def isEmpty = parts.isEmpty

    def nonEmpty = parts.nonEmpty

    def isRoot = isEmpty && isAbsolute

    def parent = {
      if (parts.isEmpty) {
        require(!isAbsolute, "invalid path")
        new Path(".." :: parts, isAbsolute)
      } else
        new Path(parts.tail, isAbsolute)
    }

    def name = {
      require(parts.nonEmpty)
      parts.head
    }

    def ext: String = {
      val n = name
      val i = n.lastIndexOf(".")
      if (i < 0) "" else n.substring(i + 1)
    }

    def withExt(ext: String): Path = {
      val n = name
      val i = n.lastIndexOf('.')
      val newName =
        if (i < 0) name + "." + ext else name.substring(0, i + 1) + ext
      new Path(newName :: parts.tail, isAbsolute)
    }

    def toAbsolute = if (isAbsolute) this else new Path(parts, true)

    def foreach(f: String => Unit): Unit = departs(parts)(f)

    def isDirectory = JFiles.isDirectory(jpath)

    def isExecutable = JFiles.isExecutable(jpath)

    def isHidden = JFiles.isHidden(jpath)

    def isReadable = JFiles.isReadable(jpath)

    def isWritable = JFiles.isWritable(jpath)

    def isRegularFile = JFiles.isRegularFile(jpath)

    def isSymbolicLink = JFiles.isSymbolicLink(jpath)

    def isSameFile(path: Path) = JFiles.isSameFile(jpath, path.jpath)

    def exists = JFiles.exists(jpath)

    def notExists = JFiles.notExists(jpath)

    def moveTo(dest: Path): Unit = {
      JFiles.move(jpath, dest.jpath)
    }

    def moveTo(dest: String): Unit =
      moveTo(pathOfExp(dest))

    def moveOverwriteTo(dest: Path): Unit = {
      dest.deleteIfExists()
      moveTo(dest)
    }

    def moveOverwriteTo(dest: String): Unit =
      moveOverwriteTo(pathOfExp(dest))

    def copyTo(dest: Path): Unit = {
      JFiles.copy(jpath, dest.jpath)
    }

    def copyTo(dest: String): Unit =
      copyTo(pathOfExp(dest))

    def copyOverwriteTo(dest: Path): Unit = {
      dest.deleteIfExists()
      copyTo(dest)
    }

    def copyOverwriteTo(dest: String): Unit =
      copyOverwriteTo(pathOfExp(dest))

    def createDir(): Unit = {
      if (!exists)
        JFiles.createDirectories(jpath)
    }

    def createFile(): Unit = JFiles.createFile(jpath)

    def delete(): Unit = JFiles.delete(jpath)

    def deleteIfExists(): Boolean = JFiles.deleteIfExists(jpath)

    def openFile(read: Boolean = false,
                 write: Boolean = false,
                 create: Boolean = false,
                 createNew: Boolean = false,
                 append: Boolean = false,
                 deleteOnClose: Boolean = false,
                 truncate: Boolean = false,
                 sync: Boolean = false,
                 dsync: Boolean = false,
                 sparse: Boolean = false) = {
      import java.nio.channels._
      import java.nio.file._
      var options: List[StandardOpenOption] = Nil
      if (read) options = StandardOpenOption.READ :: options
      if (write) options = StandardOpenOption.WRITE :: options
      if (create) options = StandardOpenOption.CREATE :: options
      if (createNew) options = StandardOpenOption.CREATE_NEW :: options
      if (append) options = StandardOpenOption.APPEND :: options
      if (deleteOnClose) options = StandardOpenOption.DELETE_ON_CLOSE :: options
      if (truncate) options = StandardOpenOption.TRUNCATE_EXISTING :: options
      if (sync) options = StandardOpenOption.SYNC :: options
      if (dsync) options = StandardOpenOption.DSYNC :: options
      if (sparse) options = StandardOpenOption.SPARSE :: options
      new File(this, AsynchronousFileChannel.open(jpath, options: _*))
    }

    def toSystemPathString() = toString(java.io.File.separator)

    override def toString() = toString(seperator)

    def toString(sep: String): String = {
      if (parts.isEmpty)
        if (isAbsolute) sep else "."
      else {
        val sb = new StringBuilder
        if (isAbsolute)
          foreach { x => sb.append(sep).append(x)}
        else {
          foreach { x => sb.append(x).append(sep)}
          if (sb.length > 0)
            sb.length -= sep.length
        }
        sb.toString()
      }
    }

    override def hashCode() = parts.hashCode()

    override def equals(obj: Any) = {
      obj != null &&
        obj.isInstanceOf[Path] && {
        val p = obj.asInstanceOf[Path]
        p.isAbsolute == isAbsolute && p.parts == parts
      }
    }
  }

  private def make(parts: List[String], abs: Boolean) = {
    require(!abs || !parts.headOption.contains(".."), "invalid path")
    new Path(parts, abs)
  }

  private def concat(parts: List[String], s: String): List[String] = concat(parts, s.split(seperator))()

  import scala.annotation.tailrec

  @tailrec
  private def concat(parts: List[String], arr: Array[String])(offset: Int = 0, end: Int = arr.length): List[String] = {
    if (offset >= end)
      parts
    else
      arr(offset) match {
        case "" | "." => concat(Nil, arr)(offset + 1, end)
        case ".." => concat(if (parts.isEmpty || parts.head == "..") ".." :: parts else parts.tail, arr)(offset + 1, end)
        case x => concat(x :: parts, arr)(offset + 1, end)
      }
  }

  private def concat(parts: List[String], p: List[String]): List[String] = {
    var ps = parts
    departs(p) {
      case ".." => ps = if (ps.isEmpty || ps.head == "..") ".." :: ps else ps.tail
      case "." | "" =>
      case x => ps = x :: ps
    }
    ps
  }

  private def departs(parts: List[String])(f: String => Unit): Unit = {
    parts match {
      case x :: tail =>
        departs(tail)(f)
        f(x)
      case Nil =>
    }
  }

  private def rel(a: List[String], b: List[String]) = {
    var a0 = a.reverse
    var b0 = b.reverse
    while (a0.nonEmpty && b0.nonEmpty && a0.head == b0.head) {
      a0 = a0.tail
      b0 = b0.tail
    }
    var c = List.empty[String]
    while (b0.nonEmpty) {
      c = ".." :: c
      b0 = b0.tail
    }
    a.reverse_:::(c)
  }

  class File(val path: Path, channel: java.nio.channels.AsynchronousFileChannel) {

    import java.nio.channels._
    import java.nio.{ByteBuffer => JByteBuffer}
    import scala.concurrent._
    import scala.util._
    import akka.util._

    def read(buf: JByteBuffer, offset: Long, length: Int = -1): Future[Long] = {
      if (length == 0)
        Future.successful(offset)
      else {
        val p = Promise[Long]()
        read1(buf, offset, length)(p.complete(_))
        p.future
      }
    }

    private def read1(buf: JByteBuffer, offset: Long, length: Int)(callback: Try[Long] => Unit): Unit = {
      require(buf.remaining() >= length, "buffer too small")
      if (length == 0) {
        callback(Success(offset))
      } else {
        val pos = buf.position()
        val limit = buf.limit()
        if (length > 0)
          buf.limit(pos + length)
        read0(buf, offset, length) { r =>
          buf.position(pos)
          buf.limit(limit)
          callback(r)
        }
      }
    }

    private def read0(buf: JByteBuffer, offset: Long, length: Int)(callback: Try[Long] => Unit): Unit = {
      channel.read(buf, offset, null, new CompletionHandler[Integer, Null] {
        def completed(v: Integer, att: Null) = {
          if (v == -1) {
            callback(Success(offset))
          } else if (v == 0) {
            read0(buf, offset, length)(callback)
          } else if (length > 0 && v < length) {
            read0(buf, offset + v, length - v)(callback)
          } else {
            callback(Success(offset + v))
          }
        }

        def failed(ex: Throwable, att: Null) = {
          callback(Failure(ex))
        }
      })
    }

    def write(buf: ByteString, offset: Long): Future[Long] = {
      val p = Promise[Long]()
      write1(buf.asByteBuffers.iterator, offset)(p.complete(_))
      p.future
    }

    def write(buf: JByteBuffer, offset: Long): Future[Long] = {
      val p = Promise[Long]()
      write0(buf, offset)(p.complete(_))
      p.future
    }

    private def write1(itor: Iterator[JByteBuffer], offset: Long)(callback: Try[Long] => Unit): Unit = {
      if (itor.hasNext) {
        write0(itor.next(), offset) {
          case Success(x) => write1(itor, x)(callback)
          case x: Failure[Long] => callback(x)
        }
      } else {
        callback(Success(offset))
      }
    }

    private def write0(buf: JByteBuffer, offset: Long)(callback: Try[Long] => Unit): Unit = {
      if (buf.remaining() > 0) {
        channel.write(buf, offset, null, new CompletionHandler[Integer, Null] {
          def completed(v: Integer, att: Null) = {
            write0(buf, offset + v)(callback)
          }

          def failed(ex: Throwable, att: Null) = {
            callback(Failure(ex))
          }
        })
      } else {
        callback(Success(offset))
      }
    }

    def close(): Unit = {
      channel.close()
    }
  }

}