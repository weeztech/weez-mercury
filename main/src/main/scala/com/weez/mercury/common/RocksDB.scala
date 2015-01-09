package com.weez.mercury.common

import java.util.Arrays

import akka.event.LoggingAdapter
import org.rocksdb
import org.rocksdb.{RocksIterator, WriteOptions, WriteBatch, ReadOptions}

object RocksDB {
  org.rocksdb.RocksDB.loadLibrary()
}

class RocksDB(path: String) extends Database {
  private val db = org.rocksdb.RocksDB.open(path)

  def withQuery(log: LoggingAdapter)(f: DBSessionQueryable => Unit) = {
    val session = new ReadSession(log)
    try {
      f(session)
    } finally {
      session.close()
    }
  }

  def withUpdate(log: LoggingAdapter)(f: DBSessionUpdatable => Unit) = {
    val session = new WriteSession(log)
    try {
      f(session)
    } finally {
      session.close()
    }
  }

  def close() = {
    db.close()
    db.dispose()
  }

  @inline def packer[T: Packer] = implicitly[Packer[T]]

  class CursorImpl[V](itor: RocksIterator, startBuf: Array[Byte], endBuf: Array[Byte]) extends Cursor[V] {
    val pv = packer[V]
    itor.seek(startBuf)

    def next(): V = {
      val v = itor.value()
      itor.next()
      pv.unapply(v)
    }

    def hasNext: Boolean = {
      itor.isValid && ByteArrayOrdering.compare(endBuf, itor.value()) >= 0
    }

    def close() = {
      itor.dispose()
    }
  }

  class ReadSession(log: LoggingAdapter) extends DBSessionQueryable {
    val snapshot = db.getSnapshot
    val readOption = new ReadOptions()
    var dbiterators: List[RocksIterator] = Nil

    readOption.setSnapshot(snapshot)

    def get[K: Packer, V: Packer](key: K): V = {
      packer[V].unapply(db.get(readOption, packer[K].apply(key)))
    }

    def get[K: Packer, V: Packer](start: K, end: K): Cursor[V] = {
      val pk = packer[K]
      val itor = db.newIterator()
      dbiterators = itor :: dbiterators
      new CursorImpl[V](itor, pk(start), pk(end)) {
        override def close() = {
          dbiterators = dbiterators.filterNot(_ eq itor)
          super.close()
        }
      }
    }

    def close(): Unit = {
      db.releaseSnapshot(snapshot)
      snapshot.dispose()
      readOption.dispose()
      dbiterators.foreach(_.dispose())
      dbiterators = Nil
    }
  }

  class WriteSession(log: LoggingAdapter) extends DBSessionUpdatable {
    val keyWrites =
      if (Util.devmode)
        scala.collection.mutable.SortedSet[Array[Byte]]()(ByteArrayOrdering)
      else
        null

    var dbiterators: List[RocksIterator] = Nil
    var writeBatch: WriteBatch = _

    def get[K: Packer, V: Packer](key: K): V = {
      val keyBuf = packer[K].apply(key)
      if (Util.devmode && keyWrites.contains(keyBuf)) {
        log.error("read the key which is written by current transaction")
      }
      packer[V].unapply(db.get(keyBuf))
    }

    def get[K: Packer, V: Packer](start: K, end: K): Cursor[V] = {
      val pk = packer[K]
      val itor = db.newIterator()
      dbiterators = itor :: dbiterators
      new CursorImpl[V](itor, pk(start), pk(end)) {
        override def close() = {
          dbiterators = dbiterators.filterNot(_ eq itor)
          super.close()
        }
      }
    }

    def put[K: Packer, V: Packer](key: K, value: V): Unit = {
      if (writeBatch == null)
        writeBatch = new WriteBatch()
      writeBatch.put(packer[K].apply(key), packer[V].apply(value))
    }

    def close(): Unit = {
      val writeOption = new WriteOptions()
      try {
        db.write(writeOption, writeBatch)
      } finally {
        writeOption.dispose()
        dbiterators.foreach(_.dispose())
        dbiterators = Nil
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
