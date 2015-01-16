package com.weez.mercury.common

import akka.event.LoggingAdapter

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

  case class Column(name: String, tpe: DBType)

  case class Collection(valueType: DBType, indexes: Seq[Index]) extends DBType

  case class Index(name: String, key: DBType)

  case class Ref(name: String) extends DBType

}

/*
object DBTypeEntityCollection extends RootCollection[DBType.Entity] {
  def name = "sys-types"

}*/

trait DatabaseChecker {

  import scala.reflect.runtime.universe._
  import scala.collection.mutable
  import org.joda.time.DateTime

  val resolvedTypes = mutable.Map[String, (DBType, String)]()
  val unresolvedTypes = mutable.Map[String, mutable.Set[String]]()
  val mirror = runtimeMirror(this.getClass.getClassLoader)

  val stringType = typeOf[String]
  val intType = typeOf[Int]
  val longType = typeOf[Long]
  val doubleType = typeOf[Double]
  val booleanType = typeOf[Boolean]
  val datetimeType = typeOf[DateTime]
  val rawType = typeOf[Array[Byte]]
  val refType = typeOf[Ref[_]]

  val entityType = typeOf[Entity]
  val typeRootColl = typeOf[RootCollection[_]]
  val indexBaseType = typeOf[IndexBase[_, _]]

  def check(log: LoggingAdapter): Unit = {
    import com.weez.mercury.ClassFinder._
    log.info("start checking database schema")
    val start = System.nanoTime()
    scalaNamesIn(classpath.filter { f =>
      f.isDirectory() || f.getName.contains("weez")
    }) foreach {
      case ScalaName(name, true, true, false) =>
        reflectDBType(mirror.staticModule(name))
      case _ =>
    }
    log.info(s"database schema checked in ${(System.nanoTime() - start) / 1000000} ms")
  }

  def reflectDBType(symbol: ModuleSymbol): DBType = {
    if (symbol.typeSignature <:< entityType) {
      val ctor = symbol.typeSignature.decl(termNames.CONSTRUCTOR).asMethod
      val dbtype = DBType.Entity(dbName(symbol.name),
        ctor.paramLists(0) map { p =>
          DBType.Column(dbName(p.name), getType(p.typeSignature, p.fullName))
        })
      resolvedTypes.put(dbtype.name, dbtype -> symbol.fullName) match {
        case Some((_, x)) => throw new Exception(s"Entity name conflict: ${symbol.fullName} and $x")
        case None =>
      }
      dbtype
    } else if (symbol.typeSignature <:< typeRootColl) {
      val rootCollType = symbol.typeSignature.baseType(typeRootColl.typeSymbol)
      val valueType = rootCollType.typeArgs(0)
      if (valueType =:= rootCollType) {
      } else {
        getType(valueType, rootCollType.typeSymbol.fullName)
      }
      symbol.typeSignature.members foreach { member =>
        if (member.isMethod) {
          val method = member.asMethod
          if (method.returnType <:< indexBaseType) {
            val indexType = method.returnType.baseType(indexBaseType.typeSymbol)
            DBType.Index(method.name.toString, null)
          }
        }
      }
    }
  }

  def dbName(name: Name) = {
    import scala.util.matching.Regex
    new Regex("[A-Z]").replaceAllIn(name.toString(), { m =>
      if (m.start == 0)
        m.matched.toLowerCase
      else
        "-" + m.matched.toLowerCase
    })
  }

  def getType(tpe: Type, ref: String): DBType = {
    if (tpe =:= stringType) DBType.String
    else if (tpe =:= intType) DBType.Int
    else if (tpe =:= longType) DBType.Long
    else if (tpe =:= doubleType) DBType.Double
    else if (tpe =:= booleanType) DBType.Boolean
    else if (tpe =:= datetimeType) DBType.DateTime
    else if (tpe =:= rawType) DBType.Raw
    else {
      val name = dbName {
        if (tpe =:= refType) {
          val entity = tpe.typeArgs(0)
          entity.typeSymbol.name
        } else
          tpe.typeSymbol.name
      }
      if (!resolvedTypes.contains(name))
        unresolvedTypes.getOrElseUpdate(name, mutable.Set()).add(ref)
      DBType.Ref(name)
    }
  }
}
