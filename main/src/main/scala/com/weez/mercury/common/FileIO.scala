package com.weez.mercury.common

import java.nio.file.{Path => JPath, Paths => JPaths, Files => JFiles, FileSystem => JFileSystem}

object FileIO {
  def pathOf(p: String) = new Path(JPaths.get(p))

  def pathOfExp(p: String) = new Path(JPaths.get(resolvePathExp(p)))

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

  final class Path private[FileIO](protected val jpath: JPath) extends Iterable[Path] {
    override def size = jpath.getNameCount

    override def iterator = new Iterator[Path] {
      var i = 0
      val c = jpath.getNameCount

      def hasNext = i < c

      def next() = {
        val jp = jpath.getName(i)
        i += 1
        of(jp)
      }
    }

    def toJavaPath = jpath

    private def of(path: String): Path = of(JPaths.get(path))

    private def of(path: JPath): Path = new Path(jpath)

    def /(path: Path): Path =
      if (path.isAbsolute) path
      else
        of(jpath.resolve(path.jpath).normalize())

    def /(path: String): Path = this / of(path)

    def ~(path: Path): Path =
      if (path.isAbsolute) path
      else
        of(jpath.resolveSibling(path.jpath).normalize())

    def ~(path: String): Path = this ~ of(path)

    def ^(path: Path): Path = of(jpath.relativize(path.jpath))

    def ^(path: String): Path = this ^ of(path)

    def in(path: Path): Boolean = jpath.startsWith(path.jpath)

    def in(path: String): Boolean = this in of(path)

    def endsWith(path: Path) = jpath.endsWith(path.jpath)

    def parent = {
      val p = jpath.getParent
      if (p == null) None else Some(of(p))
    }

    def root = {
      val p = jpath.getRoot
      if (p == null) None else Some(of(p))
    }

    def ext: String = {
      val name = jpath.getFileName.toString
      val i = name.lastIndexOf(".")
      if (i < 0) "" else name.substring(i + 1)
    }

    def withExt(ext: String): Path = {
      val name = jpath.getFileName.toString
      val i = name.lastIndexOf(".")
      val newName =
        if (i < 0) name + "." + ext else name.substring(0, i + 1) + ext
      this ~ newName
    }

    def isAbsolute = jpath.isAbsolute

    def toAbsolute = if (isAbsolute) this else of(jpath.toAbsolutePath)

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
      moveTo(of(dest))

    def moveOverwriteTo(dest: Path): Unit = {
      dest.deleteIfExists()
      moveTo(dest)
    }

    def moveOverwriteTo(dest: String): Unit =
      moveOverwriteTo(of(dest))

    def copyTo(dest: Path): Unit = {
      JFiles.copy(jpath, dest.jpath)
    }

    def copyTo(dest: String): Unit =
      copyTo(of(dest))

    def copyOverwriteTo(dest: Path): Unit = {
      dest.deleteIfExists()
      copyTo(dest)
    }

    def copyOverwriteTo(dest: String): Unit =
      copyOverwriteTo(of(dest))

    def createDir(): Unit = {
      if (!exists) {
        JFiles.createDirectories(jpath)
      }
    }

    def createFile(): Unit = {
      JFiles.createFile(jpath)
    }

    def streamRead() = JFiles.newInputStream(jpath)

    def streamWrite() = JFiles.newOutputStream(jpath)

    def open() = JFiles.newByteChannel(jpath)

    def delete(): Unit = {
      JFiles.delete(jpath)
    }

    def deleteIfExists(): Boolean = {
      JFiles.deleteIfExists(jpath)
    }

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
      new File(AsynchronousFileChannel.open(jpath, options: _*))
    }

    override def toString() = jpath.toString

    override def hashCode() = jpath.hashCode()

    override def equals(obj: Any) = {
      obj != null &&
        obj.isInstanceOf[Path] &&
        obj.asInstanceOf[Path].jpath.equals(jpath)
    }
  }

  class File(channel: java.nio.channels.AsynchronousFileChannel) {

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
  }

}