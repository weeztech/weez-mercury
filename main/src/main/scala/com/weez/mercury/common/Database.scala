package com.weez.mercury.common

import akka.event.LoggingAdapter

trait Database {
  def createSession(): DBSession

  def close(): Unit
}

object Database {
  val KEY_OBJECT_ID_COUNTER = "object-id-counter"
}

trait DatabaseFactory {
  def createNew(path: String): Database

  def open(path: String): Database

  def delete(path: String): Unit
}

trait DBSession {
  def withTransaction(log: LoggingAdapter)(f: DBTransaction => Unit): Unit

  def close(): Unit
}

trait DBTransaction {
  def get[K, V](key: K)(implicit pk: Packer[K], pv: Packer[V]): V

  def get[K, V](start: K, end: K)(implicit pk: Packer[K], pv: Packer[V]): DBCursor[K, V]

  def put[K, V](key: K, value: V)(implicit pk: Packer[K], pv: Packer[V]): Unit
}

trait DBCursor[K, V] extends Iterator[(K, V)] {
  def close(): Unit
}
