package com.weez.mercury.common

import java.util.Arrays
import akka.event.LoggingAdapter
import org.rocksdb._

object RocksDBBackend extends DatabaseFactory {
  RocksDB.loadLibrary()

  def createNew(path: String): Database = {
    import java.nio.file._
    val actualPath = Util.resolvePath(path)
    val p = Paths.get(actualPath)
    if (Files.exists(p, LinkOption.NOFOLLOW_LINKS))
      throw new Exception("already exists")
    Files.createDirectories(p)
    new RocksDBBackend(actualPath)
  }

  def open(path: String): Database = {
    import java.nio.file._
    val p = Util.resolvePath(path)
    if (!Files.isDirectory(Paths.get(p)))
      throw new Exception("not a database directory")
    new RocksDBBackend(p)
  }

  def delete(path: String) =
    Util.deleteDirectory(Util.resolvePath(path))
}

private class RocksDBBackend(path: String) extends Database {

  private val db = RocksDB.open(path)

  def createSession() =
    new DBSession {
      def close() = ()

      def withTransaction[T](log: LoggingAdapter)(f: DBTransaction => T): T = {
        val trans = if (Util.devmode) new DevTransaction(log) else new Transaction(log)
        try {
          val ret = f(trans)
          trans.commit()
          ret
        } finally {
          trans.close()
        }
      }
    }

  def close() = {
    db.close()
    db.dispose()
  }

  @inline def packer[T: Packer] = implicitly[Packer[T]]

  class CursorImpl(itor: RocksIterator) extends DBCursor {

    private var valid: Boolean = _ //cache


    def seek(key: Array[Byte]) = {
      itor.seek(key)
      valid = itor.isValid
      valid
    }


    def next(n: Int) = {
      var i = 0
      if (n > 0) {
        while (i < n && valid) {
          itor.next()
          valid = itor.isValid
          i += 1
        }
      } else if (n < 0) {
        while (i > n && valid) {
          itor.prev()
          valid = itor.isValid
          i -= 1
        }
      }
      valid
    }

    def value() = itor.value()

    def key() = itor.key()

    def isValid = valid

    def close() = itor.dispose()
  }

  class Transaction(log: LoggingAdapter) extends DBTransaction {
    val snapshot = db.getSnapshot
    val readOption = new ReadOptions()

    var dbIterators: List[RocksIterator] = Nil
    var writeBatch: WriteBatch = _

    def get[K, V](key: K)(implicit pk: Packer[K], pv: Packer[V]) = {
      val keyBuf = pk(key)
      val buf = db.get(readOption, keyBuf)
      if (buf == null) None else Some(pv.unapply(buf))
    }


    override def exists[K](key: K)(implicit pk: Packer[K]): Boolean = {
      db.get(readOption, pk(key)) != null
    }

    override def del[K](key: K)(implicit pk: Packer[K]): Unit = {
      if (writeBatch == null)
        writeBatch = new WriteBatch()
      writeBatch.remove(pk(key))
    }

    def newCursor(): DBCursor = {
      val iterator = db.newIterator()
      dbIterators = iterator :: dbIterators
      new CursorImpl(iterator) {
        override def close() = {
          dbIterators = dbIterators.filterNot(_ eq iterator)
          super.close()
        }
      }
    }

    def put[K: Packer, V: Packer](key: K, value: V): Unit = {
      if (writeBatch == null)
        writeBatch = new WriteBatch()
      writeBatch.put(packer[K].apply(key), packer[V].apply(value))
    }

    def commit() = {
      if (writeBatch != null) {
        val writeOption = new WriteOptions()
        try {
          db.write(writeOption, writeBatch)
        } finally {
          writeOption.dispose()
        }
      }
    }

    def close(): Unit = {
      dbIterators.foreach(_.dispose())
      dbIterators = Nil
      db.releaseSnapshot(snapshot)
      snapshot.dispose()
      readOption.dispose()
      if (writeBatch != null) {
        writeBatch.dispose()
        writeBatch = null
      }
    }
  }

  class DevTransaction(log: LoggingAdapter) extends Transaction(log) {
    val keyWrites = scala.collection.mutable.Set[Any]()

    override def get[K: Packer, V: Packer](key: K) = {
      if (keyWrites.contains(key)) {
        log.error("read the key which is written by current transaction")
      }
      super.get[K, V](key)
    }


    override def newCursor() = {
      val c = super.newCursor()
      new DBCursor {
        override def seek(key: Array[Byte]) = c.seek(key)

        override def next(step: Int) = {
          if (c.next(step)) {
            // TODO fix
            if (keyWrites.contains(c.key())) {
              log.error("read the key which is written by current transaction")
            }
          }
          c.isValid
        }

        override def isValid: Boolean = c.isValid

        override def key(): Array[Byte] = c.key()

        override def value(): Array[Byte] = c.value()

        override def close() = c.close()
      }
    }
  }

  object ByteArrayOrdering extends Ordering[Array[Byte]] {
    def compare(x: Array[Byte], y: Array[Byte]) = {
      var i = 0
      val len = x.length.min(y.length)
      var c = 0
      while (c == 0 && i < len) {
        c = x(i) - y(i)
        i += 1
      }
      if (c == 0)
        x.length - y.length
      else
        c
    }
  }

}
