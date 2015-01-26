package com.weez.mercury.common

import akka.event.LoggingAdapter

trait Database {
  def createSession(): DBSession

  def close(): Unit
}

trait DatabaseFactory {
  def createNew(path: String): Database

  def open(path: String): Database

  def delete(path: String): Unit
}

trait DBSession {
  def newTransaction(log: LoggingAdapter): DBTransaction

  def close(): Unit
}

trait DBTransaction {
  def get(key: Array[Byte]): Array[Byte]

  def newCursor(): DBCursor

  def put(key: Array[Byte], value: Array[Byte]): Unit

  def exists(key: Array[Byte]): Boolean

  def del(key: Array[Byte]): Unit

  def commit(): Unit

  def close()
}

trait DBCursor {

  def seek(key: Array[Byte]): Boolean

  def next(n: Int): Boolean

  def isValid: Boolean

  def key(): Array[Byte]

  def value(): Array[Byte]

  def close(): Unit
}

case class RefSome[T <: Entity](id: Long) extends Ref[T] {
  def apply()(implicit db: DBSessionQueryable): T = EntityCollections.getEntity(id)
}

case object RefEmpty extends Ref[Nothing] {
  def id = 0L

  def apply()(implicit db: DBSessionQueryable) = throw new NoSuchElementException("RefEmpty()")
}
