package com.weez.mercury.common

import akka.event.LoggingAdapter

trait Database {
  def createSession(): DBSession
}

trait DBSession {
  def withTransaction(log: LoggingAdapter)(f: DBTransaction => Unit): Unit

  def close(): Unit
}

trait DBTransaction {
  def get[K, V](key: K)(implicit pk: Packer[K], pv: Packer[V]): V

  def get[K, V](start: K, end: K)(implicit pk: Packer[K], pv: Packer[V]): Cursor[K, V]

  def put[K, V](key: K, value: V)(implicit pk: Packer[K], pv: Packer[V]): Unit
}

