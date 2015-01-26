package com.weez.mercury.common

import akka.event.LoggingAdapter
import org.rocksdb._

class RocksDBDatabaseFactory(g: GlobalSettings) extends DatabaseFactory {
  def createNew(path: String): Database = {
    import java.nio.file._
    val actualPath = Util.resolvePath(path)
    val p = Paths.get(actualPath)
    if (Files.exists(p, LinkOption.NOFOLLOW_LINKS))
      throw new Exception("already exists")
    Files.createDirectories(p)
    new RocksDBDatabase(g, actualPath)
  }

  def open(path: String): Database = {
    import java.nio.file._
    val p = Util.resolvePath(path)
    if (!Files.isDirectory(Paths.get(p)))
      throw new Exception("not a database directory")
    new RocksDBDatabase(g, p)
  }

  def delete(path: String) =
    Util.deleteDirectory(Util.resolvePath(path))
}

private class RocksDBDatabase(g: GlobalSettings, path: String) extends Database {
  RocksDB.loadLibrary()

  private val db = RocksDB.open(path)

  def createSession() =
    new DBSession {
      def close() = ()

      def newTransaction(log: LoggingAdapter): DBTransaction = {
        if (g.devmode) new DevTransaction(log) else new Transaction(log)
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

    def get(key: Array[Byte]) = db.get(readOption, key)

    def exists(key: Array[Byte]): Boolean = {
      db.get(readOption, key) != null
    }

    def del(key: Array[Byte]): Unit = {
      if (writeBatch == null)
        writeBatch = new WriteBatch()
      writeBatch.remove(key)
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

    def put(key: Array[Byte], value: Array[Byte]): Unit = {
      if (writeBatch == null)
        writeBatch = new WriteBatch()
      writeBatch.put(key, value)
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
    val keyWrites = scala.collection.mutable.SortedSet[Array[Byte]]()(
      new Ordering[Array[Byte]] {
        def compare(x: Array[Byte], y: Array[Byte]) = Util.compareUInt8s(x, y)
      })

    override def get(key: Array[Byte]) = {
      if (keyWrites.contains(key)) {
        log.error("read the key which is written by current transaction")
      }
      super.get(key)
    }


    override def newCursor() = {
      val c = super.newCursor()
      new DBCursor {
        override def seek(key: Array[Byte]) = c.seek(key)

        override def next(step: Int) = {
          if (c.next(step)) {
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

}
