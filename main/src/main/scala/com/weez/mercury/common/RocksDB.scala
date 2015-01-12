package com.weez.mercury.common

import java.util.Arrays
import akka.event.LoggingAdapter
import org.rocksdb._

object RocksDBBackend extends DatabaseFactory {
  RocksDB.loadLibrary()

  def createNew(path: String): Database = {
    import java.nio.file._
    val p = Util.resolvePath(path)
    if (Files.exists(Paths.get(p), LinkOption.NOFOLLOW_LINKS))
      throw new Exception("already exists")
    new RocksDBBackend(p)
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

      def withTransaction(log: LoggingAdapter)(f: DBTransaction => Unit) = {
        val trans = if (Util.devmode) new DevTransaction(log) else new Transaction(log)
        try {
          f(trans)
          trans.commit()
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

  class CursorImpl[K, V](itor: RocksIterator)(implicit pk: Packer[K], pv: Packer[V]) extends DBCursor[K, V] {

    def seek(key: K) = {
      itor.seek(pk(key))
      itor.isValid
    }

    def next(): (K, V) = {
      val k = itor.key()
      val v = itor.value()
      itor.next()
      pk.unapply(k) -> pv.unapply(v)
    }

    def hasNext: Boolean = {
      itor.isValid
    }

    def close() = {
      itor.dispose()
    }
  }

  class Transaction(log: LoggingAdapter) extends DBTransaction {
    val snapshot = db.getSnapshot
    val readOption = new ReadOptions()

    var dbIterators: List[RocksIterator] = Nil
    var writeBatch: WriteBatch = _

    def get[K, V](key: K)(implicit pk: Packer[K], pv: Packer[V]): V = {
      val keyBuf = pk(key)
      pv.unapply(db.get(readOption, keyBuf))
    }

    def newCursor[K, V](implicit pk: Packer[K], pv: Packer[V]): DBCursor[K, V] = {
      val iterator = db.newIterator()
      dbIterators = iterator :: dbIterators
      new CursorImpl[K, V](iterator) {
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

    override def get[K: Packer, V: Packer](key: K): V = {
      if (keyWrites.contains(key)) {
        log.error("read the key which is written by current transaction")
      }
      super.get[K, V](key)
    }


    override def newCursor[K: Packer, V: Packer] = {
      val c = super.newCursor[K, V]
      new DBCursor[K, V] {
        def seek(key: K) = c.seek(key)

        def next() = {
          val tp = c.next()
          if (keyWrites.contains(tp._1)) {
            log.error("read the key which is written by current transaction")
          }
          tp
        }

        def hasNext: Boolean = c.hasNext

        def close() = c.close()
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
