package com.weez.mercury.stock

import java.util

import org.rocksdb._

import java.util.Random

object A extends App {
  // the Options class contains a set of configurable DB options
  // that determines the behavior of a database.
  val options = new Options().setCreateIfMissing(true)
  options.setUseFsync(false)
  val db = RocksDB.open(options, "/Users/gaojingxin/rocksDB")
  try {
    var i = 1000000
    val key = new Array[Byte](400)
    val value = new Array[Byte](10)
    val r = new Random()
    val start = System.currentTimeMillis()
    val rs = db.newIterator()
    while (i > 0) {
      r.nextBytes(key)
      r.nextBytes(value)
      rs.seek(key)
      i -= 1
    }
    System.out.println(System.currentTimeMillis() - start)

  } finally {
    db.close()
  }
}

trait Template {

  def build(stringBuilder: StringBuilder): Unit

  def ++(right: Any) = right match {
    case p: Parts =>
      Parts(this.asInstanceOf[Any] +: p.parts)
    case _ =>
      Parts(Seq(this, right))
  }

  override def toString = {
    val sb = new StringBuilder()
    this.build(sb)
    sb.toString()
  }
}

case class Parts(parts: Seq[Any]) extends Template {
  override def ++(right: Any) = right match {
    case p: Parts =>
      Parts(this.parts ++ p.parts)
    case _ =>
      super.++(right)
  }


  override def build(stringBuilder: StringBuilder): Unit = {
    for (part <- parts) {
      part match {
        case p: Template =>
          p.build(stringBuilder)
        case _ =>
          stringBuilder.append(part.toString)
      }
    }
  }
}

class Indexer(from: Int, to: Int) extends Template {
  def reset(): Unit = {
    index = from
  }

  def next() = {
    if (index < to) {
      index += 1
      true
    } else {
      false
    }
  }

  private var index: Int = from

  def hasNext = index < to

  def build(stringBuilder: StringBuilder): Unit = {
    stringBuilder.append(index)
  }
}


class Repeat private(separator: String, indexer: Indexer, tmp: Template) extends Template {

  override def build(stringBuilder: StringBuilder): Unit = {
    indexer.reset()
    while (indexer.next()) {
      tmp.build(stringBuilder)
      if (indexer.hasNext) {
        stringBuilder.append(separator)
      }
    }
  }
}

object Repeat {

  def apply(to: Int, build: Indexer => Template, separator: String): Repeat = {
    apply(0, to, build, separator)
  }

  def apply(from: Int, to: Int, build: Indexer => Template, separator: String = ","): Repeat = {
    val indexer = new Indexer(from, to)
    new Repeat(separator, indexer, build(indexer))
  }

  implicit class TemplateContext(val sc: StringContext) extends AnyVal {
    def t(params: Any*) = {
      val newParts = collection.mutable.ArrayStack[Any]()
      var i = 0
      while (i < params.length) {
        newParts.push(sc.parts(i))
        newParts.push(params(i))
        i += 1
      }
      newParts.push(sc.parts.last)
      new Parts(newParts.toSeq)
    }
  }

}

import Repeat._

object Test {
  Repeat(1, 10, r1 =>
    t"abc$r1${Repeat(2, 10, r2 => t"$r1$r2")}"
  ).toString
}