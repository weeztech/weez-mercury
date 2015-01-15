package com.weez.mercury.common

import akka.event.LoggingAdapter
import com.weez.mercury.ClassFinder

trait DBSchema {
  def newEntityID(): Long

  def getHostCollection(name: String): DBType.Collection
}

sealed trait DBType

object DBType extends DBPrimaryTypes with DBExtendTypes

trait DBPrimaryTypes {

  object String extends DBType

  object Int extends DBType

  object Long extends DBType

  object Double extends DBType

  object Boolean extends DBType

  object DateTime extends DBType

  object Raw extends DBType

}

trait DBExtendTypes {

  case class Tuple(parts: Seq[DBType]) extends DBType

  case class Entity(name: String, columns: Seq[Column]) extends DBType

  case class Column(name: String, tpe: Int)

  case class Collection(id: Int, valueType: DBType, indexes: Seq[Index]) extends DBType

  case class Index(id: Int, name: String, key: DBType)

}

/*
object DBTypeEntityCollection extends RootCollection[DBType.Entity] {
  def name = "sys-types"

}*/

trait DatabaseChecker {
  def check(log: LoggingAdapter): Unit = {
    import scala.reflect.runtime.{universe => ru}
    import com.weez.mercury.ClassFinder._
    log.info("start checking database schema")
    val start = System.nanoTime()
    val mirror = ru.runtimeMirror(this.getClass.getClassLoader)
    scalaNamesIn(classpath.filter { f =>
      f.isDirectory() || f.getName.contains("weez")
    }) foreach {
      case ScalaName(name, true, true, false) =>
        val symbol = mirror.staticModule(name)
        if (symbol.typeSignature <:< ru.typeOf[DBObjectType[_]]) {
          mirror.reflectModule(symbol).instance.asInstanceOf[DBObjectType]
        }
      case _ =>
    }
    log.info(s"database schema checked in ${(System.nanoTime() - start) / 1000000} ms")
  }
}
