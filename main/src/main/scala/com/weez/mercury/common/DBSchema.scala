package com.weez.mercury.common

import akka.event.LoggingAdapter

trait DBSchema {
  def newEntityID(): Long

  def getRootCollectionMeta(name: String)(implicit db: DBSessionQueryable): RootCollectionMeta
}

sealed trait DBType

object DBType {

  case class SimpleType private[DBType](typeCode: Int) extends DBType

  private object SimpleType

  val String = SimpleType(1)
  val Int = SimpleType(2)
  val Long = SimpleType(3)
  val Double = SimpleType(4)
  val Boolean = SimpleType(5)
  val DateTime = SimpleType(6)
  val Raw = SimpleType(7)

  def fromTypeCode(typeCode: Int): SimpleType = {
    typeCode match {
      case String.typeCode => String
      case Int.typeCode => Int
      case Long.typeCode => Long
      case Double.typeCode => Double
      case Boolean.typeCode => Boolean
      case DateTime.typeCode => DateTime
      case Raw.typeCode => Raw
    }
  }

  @dbtype
  case class Ref(name: String) extends DBType

  val simpleTypePacker = Packer[SimpleType, Int]((tpe: SimpleType) => tpe.typeCode, i => fromTypeCode(i))

  implicit object DBTypePacker extends Packer[DBType] {
    def pack(value: DBType, buf: Array[Byte], offset: Int) = {
      value match {
        case x: SimpleType => simpleTypePacker.pack(x, buf, offset)
        case x: Ref => Packer[Ref].pack(x, buf, offset)
        case _ => throw new IllegalArgumentException()
      }
    }

    def packLength(value: DBType) = {
      value match {
        case x: SimpleType => simpleTypePacker.packLength(x)
        case x: Ref => Packer[Ref].packLength(x)
        case _ => throw new IllegalArgumentException()
      }
    }

    def unpack(buf: Array[Byte], offset: Int, length: Int): DBType = {
      buf(offset) match {
        case Packer.TYPE_UINT32 => simpleTypePacker.unpack(buf, offset, length)
        case Packer.TYPE_TUPLE => Packer[Ref].unpack(buf, offset, length)
      }
    }

    def unpackLength(buf: Array[Byte], offset: Int) = {
      buf(offset) match {
        case Packer.TYPE_UINT32 => simpleTypePacker.unpackLength(buf, offset)
        case Packer.TYPE_TUPLE => Packer[Ref].unpackLength(buf, offset)
      }
    }
  }

  case class Tuple(parts: Seq[DBType]) extends DBType

  trait Named {
    def name: String
  }

  @dbtype
  case class Column(name: String, tpe: DBType) extends Named

  @dbtype
  case class Entity(name: String, columns: Seq[Column]) extends DBType with Named

  @dbtype
  case class Index(name: String, key: DBType, unique: Boolean) extends Named

  @dbtype
  case class Collection(name: String, valueType: DBType, indexes: Seq[Index], isRoot: Boolean) extends DBType with Named

}

@dbtype
case class IndexMeta(id: Long, name: String, prefix: Int)

@dbtype
case class RootCollectionMeta(id: Long, name: String, prefix: Int, indexes: Seq[IndexMeta])

object RootCollectionMetas extends RootCollection[RootCollectionMeta] {
  val name = "root-collection-metas"

  val byName = defUniqueIndex("by-name", _.name)
}

class DBSchemaImpl extends DBSchema {
  val KEY_OBJECT_ID_COUNTER = "object-id-counter"

  val dbSession: DBSession = null
  val log: LoggingAdapter = null
  val rootCollectionMeta = RootCollectionMeta(0, "root-collection-metas", 0, Seq(IndexMeta(0, "by-name", 0)))

  private var allocIdLimit = {
    dbSession.withTransaction(log) { trans =>
      trans.get[String, Long](KEY_OBJECT_ID_COUNTER).get
    }
  }
  private var currentId = allocIdLimit
  private val allocIdBatch = 1024

  def newEntityID() = {
    this.synchronized {
      if (currentId == allocIdLimit) {
        allocIdLimit += allocIdBatch
        dbSession.withTransaction(log) { trans =>
          trans.put(KEY_OBJECT_ID_COUNTER, allocIdLimit)
        }
      }
      currentId += 1
      currentId
    }
  }

  def getRootCollectionMeta(name: String)(implicit db: DBSessionQueryable) = {
    if (name == rootCollectionMeta.name) {
      rootCollectionMeta
    } else {
      RootCollectionMetas.byName(name) match {
        case Some(x) => x
        case None => throw new Error("meta of root-collection not found")
      }
    }
  }

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
  val collType = typeOf[EntityCollection[_]]
  val rootCollType = typeOf[RootCollection[_]]
  val indexBaseType = typeOf[IndexBase[_, _]]
  val uniqueIndexType = typeOf[UniqueIndex[_, _]]

  def collectDBTypes(log: LoggingAdapter): Unit = {
    import com.weez.mercury.ClassFinder._
    log.info("resolving dbtypes ...")
    val start = System.nanoTime()
    scalaNamesIn(classpath.filter { f =>
      f.isDirectory || f.getName.contains("weez")
    }) foreach {
      case ScalaName(name, true, true, false) =>
        reflectDBType(mirror.staticModule(name))
      case _ =>
    }
    log.info(s"dbtypes resolved in ${(System.nanoTime() - start) / 1000000} ms")
    if (unresolvedTypes.nonEmpty) {
      val sb = new StringBuilder
      sb.append("following types unresolved and used as dbtypes:\r\n")
      unresolvedTypes foreach {
        case (symbolName, refs) =>
          sb.append(s"\t'$symbolName' used by\r\n")
          refs foreach { ref =>
            sb.append(s"\t\t$ref\r\n")
          }
      }
      log.error(sb.toString())
      throw new Error("unresolve types used as dbtypes!")
    }
  }

  def check(): Unit = ???


  def reflectDBType(symbol: ModuleSymbol): DBType = {
    val dbtype: DBType with DBType.Named =
      if (symbol.typeSignature <:< entityType) {
        val ctor = symbol.typeSignature.decl(termNames.CONSTRUCTOR).asMethod
        DBType.Entity(dbName(symbol.name),
          ctor.paramLists(0) map { p =>
            DBType.Column(dbName(p.name), getType(p.typeSignature, p.fullName))
          })
      } else if (symbol.typeSignature <:< collType) {
        val baseType = symbol.typeSignature.baseType(collType.typeSymbol)
        val valueType = getType(baseType.typeArgs(0), symbol.fullName)
        val builder = Seq.newBuilder[DBType.Index]
        symbol.typeSignature.members foreach { member =>
          if (member.isMethod) {
            val tpe = member.asMethod.returnType
            if (tpe <:< indexBaseType) {
              val indexType = tpe.baseType(indexBaseType.typeSymbol)
              builder += DBType.Index(dbName(member.name),
                getType(indexType.typeArgs(0), member.fullName),
                tpe <:< uniqueIndexType)
            }
          }
        }
        DBType.Collection(dbName(symbol.name), valueType, builder.result(), symbol.typeSignature <:< rootCollType)
      } else null
    if (dbtype != null) {
      resolvedTypes.put(dbtype.name, dbtype -> symbol.fullName) match {
        case Some((_, x)) => throw new Error(s"db-name conflict: ${symbol.fullName} and $x")
        case None => unresolvedTypes.remove(symbol.fullName)
      }
    }
    dbtype
  }

  def dbName(name: Name) = {
    import scala.util.matching.Regex
    new Regex("[A-Z]+").replaceAllIn(name.toString(), { m =>
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
    else if (tpe.typeSymbol.fullName startsWith "scala.Tuple") {
      DBType.Tuple(tpe.typeArgs.map(getType(_, ref)))
    } else {
      val name = dbName {
        if (tpe =:= refType) {
          val entity = tpe.typeArgs(0)
          entity.typeSymbol.name
        } else
          tpe.typeSymbol.name
      }
      if (!resolvedTypes.contains(name))
        unresolvedTypes.getOrElseUpdate(tpe.typeSymbol.fullName, mutable.Set()).add(ref)
      DBType.Ref(name)
    }
  }
}
