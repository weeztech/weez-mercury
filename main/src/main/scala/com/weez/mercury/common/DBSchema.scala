package com.weez.mercury.common

import akka.event.LoggingAdapter

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

  case class Tuple(parts: Seq[DBType]) extends DBType

  case class Ref(name: String) extends DBType

  implicit object DBTypePacker extends Packer[DBType] {
    val seqPacker = Packer.of[Seq[DBType]]

    def pack(value: DBType, buf: Array[Byte], offset: Int) = {
      value match {
        case SimpleType(x) => Packer.IntPacker.pack(x, buf, offset)
        case Ref(x) => Packer.StringPacker.pack(x, buf, offset)
        case Tuple(x) => seqPacker.pack(x, buf, offset)
        case _ => throw new IllegalArgumentException()
      }
    }

    def packLength(value: DBType) = {
      value match {
        case SimpleType(x) => Packer.IntPacker.packLength(x)
        case Ref(x) => Packer.StringPacker.packLength(x)
        case Tuple(x) => seqPacker.packLength(x)
        case _ => throw new IllegalArgumentException()
      }
    }

    def unpack(buf: Array[Byte], offset: Int, length: Int): DBType = {
      buf(offset) match {
        case Packer.TYPE_UINT32 => fromTypeCode(Packer.IntPacker.unpack(buf, offset, length))
        case Packer.TYPE_STRING => Ref(Packer.StringPacker.unpack(buf, offset, length))
        case Packer.TYPE_TUPLE => Tuple(seqPacker.unpack(buf, offset, length))
      }
    }

    def unpackLength(buf: Array[Byte], offset: Int) = {
      buf(offset) match {
        case Packer.TYPE_UINT32 => Packer.IntPacker.unpackLength(buf, offset)
        case Packer.TYPE_STRING => Packer.StringPacker.unpackLength(buf, offset)
        case Packer.TYPE_TUPLE => seqPacker.unpackLength(buf, offset)
      }
    }
  }

  trait Named {
    def name: String
  }

  @packable
  case class ColumnMeta(name: String, tpe: DBType) extends Named

  @packable
  case class EntityMeta(id: Long, name: String, columns: Seq[ColumnMeta]) extends DBType with Named with Entity

  @packable
  case class IndexMeta(name: String, key: DBType, unique: Boolean, prefix: Int) extends Named

  @packable
  case class CollectionMeta(id: Long, name: String, valueType: DBType, indexes: Seq[IndexMeta], isRoot: Boolean, prefix: Int) extends DBType with Named with Entity {
    def indexPrefixOf(idx: String) = indexes.find(_.name == name).get.prefix
  }

}

object EntityMetaCollection extends RootCollection[DBType.EntityMeta] {
  def name = "entity-meta-collection"

  val byName = defUniqueIndex("by-name", _.name)
}

object CollectionMetaCollection extends RootCollection[DBType.CollectionMeta] {
  def name = "collection-meta-collection"

  val byName = defUniqueIndex("by-name", _.name)
}

class DBTypeCollector {

  import scala.reflect.runtime.universe._
  import scala.collection.mutable
  import org.joda.time.DateTime

  val resolvedTypes = mutable.Map[String, DBType]()
  val resolvedScalaTypes = mutable.Map[String, String]()
  val unresolvedScalaTypes = mutable.Map[String, mutable.Set[String]]()

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
    val mirror = runtimeMirror(this.getClass.getClassLoader)
    scalaNamesIn(classpath.filter { f =>
      f.isDirectory || f.getName.contains("weez")
    }) foreach {
      case ScalaName(name, true, true, false) =>
        reflectDBType(mirror.staticModule(name))
      case _ =>
    }
    log.info(s"dbtypes resolved in ${(System.nanoTime() - start) / 1000000} ms")
    if (unresolvedScalaTypes.nonEmpty) {
      val sb = new StringBuilder
      sb.append("following types unresolved and used as dbtypes:\r\n")
      unresolvedScalaTypes foreach {
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

  def reflectDBType(symbol: ModuleSymbol): DBType = {
    val dbtype: DBType with DBType.Named =
      if (symbol.typeSignature <:< entityType) {
        val ctor = symbol.typeSignature.decl(termNames.CONSTRUCTOR).asMethod
        DBType.EntityMeta(0, dbName(symbol.name),
          ctor.paramLists(0) map { p =>
            DBType.ColumnMeta(dbName(p.name), getType(p.typeSignature, p.fullName))
          })
      } else if (symbol.typeSignature <:< collType) {
        val baseType = symbol.typeSignature.baseType(collType.typeSymbol)
        val valueType = getType(baseType.typeArgs(0), symbol.fullName)
        val builder = Seq.newBuilder[DBType.IndexMeta]
        symbol.typeSignature.members foreach { member =>
          if (member.isMethod) {
            val tpe = member.asMethod.returnType
            if (tpe <:< indexBaseType) {
              val indexType = tpe.baseType(indexBaseType.typeSymbol)
              builder += DBType.IndexMeta(dbName(member.name),
                getType(indexType.typeArgs(0), member.fullName),
                tpe <:< uniqueIndexType, 0)
            }
          }
        }
        DBType.CollectionMeta(0, dbName(symbol.name), valueType, builder.result(), symbol.typeSignature <:< rootCollType, 0)
      } else null
    if (dbtype != null) {
      resolvedScalaTypes.put(dbtype.name, symbol.fullName) match {
        case Some(x) => throw new Error(s"db-name conflict: ${symbol.fullName} and $x")
        case None =>
          resolvedTypes.put(dbtype.name, dbtype)
          unresolvedScalaTypes.remove(symbol.fullName)
      }
    }
    dbtype
  }

  def dbName(name: Name) = Util.camelCase2seqStyle(name.toString)

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
        unresolvedScalaTypes.getOrElseUpdate(tpe.typeSymbol.fullName, mutable.Set()).add(ref)
      DBType.Ref(name)
    }
  }
}
