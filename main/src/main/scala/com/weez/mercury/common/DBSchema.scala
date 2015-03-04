package com.weez.mercury.common

import akka.event.LoggingAdapter
import com.weez.mercury.imports.packable

sealed trait DBType

object DBType {

  sealed trait DBTypeRef

  import shapeless._

  implicit val typeRefPacker: Packer[DBTypeRef] = Packer.poly[DBTypeRef,
    String.type :: Int.type :: Long.type :: Double.type :: Boolean.type :: DateTime.type :: Raw.type :: AbstractEntity.type ::
      Coll :: Tuple :: Ref :: Struct :: HNil].asInstanceOf[Packer[DBTypeRef]]

  sealed abstract class SimpleType(val typeCode: Int, name: String) extends DBType with DBTypeRef {
    override def toString = name
  }

  object String extends SimpleType(1, "String")

  object Int extends SimpleType(2, "Int")

  object Long extends SimpleType(3, "Long")

  object Double extends SimpleType(4, "Double")

  object Boolean extends SimpleType(5, "Boolean")

  object DateTime extends SimpleType(6, "DateTime")

  object Raw extends SimpleType(7, "Raw")

  object AbstractEntity extends SimpleType(10, "AbstractEntity")

  def fromTypeCode(typeCode: Int): SimpleType = {
    typeCode match {
      case String.typeCode => String
      case Int.typeCode => Int
      case Long.typeCode => Long
      case Double.typeCode => Double
      case Boolean.typeCode => Boolean
      case DateTime.typeCode => DateTime
      case Raw.typeCode => Raw
      case AbstractEntity.typeCode => AbstractEntity
    }
  }

  @packable
  case class Coll(element: DBTypeRef) extends DBType with DBTypeRef

  @packable
  case class Tuple(parts: Seq[DBTypeRef]) extends DBType with DBTypeRef

  @packable
  case class Ref(tpe: DBTypeRef) extends DBType with DBTypeRef

  @packable
  case class Struct(name: String) extends DBTypeRef

  sealed trait Meta extends DBType with Entity {
    def name: String
  }

  object Meta {
    implicit val packer: Packer[Meta] = Packer.poly[Meta,
      StructMeta :: InterfaceMeta :: ValueMeta :: CollectionMeta :: DataViewMeta :: HNil].asInstanceOf[Packer[Meta]]
  }

  @packable
  case class ColumnMeta(name: String, tpe: DBTypeRef)

  @packable
  case class StructMeta(name: String, columns: Seq[ColumnMeta], interfaces: Seq[String], isEntity: Boolean) extends Meta

  @packable
  case class InterfaceMeta(name: String, isSealed: Boolean, subs: Seq[String], interfaces: Seq[String]) extends Meta

  @packable
  case class ValueMeta(name: String, interfaces: Seq[String]) extends Meta

  @packable
  case class IndexMeta(name: String, key: DBTypeRef, unique: Boolean, prefix: Int)

  @packable
  case class CollectionMeta(name: String, valueType: DBTypeRef, indexes: Seq[IndexMeta], isRoot: Boolean, prefix: Int) extends Meta {
    def indexPrefixOf(name: String) = indexes.find(_.name == name).get.prefix
  }

  @packable
  case class DataViewMeta(name: String, prefix: Int) extends Meta

}

object MetaCollection extends RootCollection[DBType.Meta] {
  def name = "meta-collection"

  val byName = defUniqueIndex("by-name", _.name)
  val prefix = 1
}

object DBMetas {

  import DBType._

  val metaCollectionMeta = CollectionMeta(
    MetaCollection.name,
    Struct("meta"),
    IndexMeta("by-name", String, unique = true, 2) :: Nil,
    isRoot = true, MetaCollection.prefix)
}

import scala.reflect.runtime.universe._

class DBTypeCollector(types: Map[String, Seq[Symbol]]) {

  import scala.collection.mutable
  import org.joda.time.DateTime

  val resolvedMetas = mutable.Map[String, DBType.Meta]()
  val resolvedStructTypes = mutable.Map[String, String]()
  val unresolvedStructTypes = mutable.Map[String, mutable.Set[String]]()
  val delayResolvedStructTypes = mutable.Set[Symbol]()

  val stringType = typeOf[String]
  val datetimeType = typeOf[DateTime]
  val rawType = typeOf[Array[Byte]]
  val refType = typeOf[Ref[_]]
  val traversableType = typeOf[Traversable[_]]

  val entityType = typeOf[Entity]
  val collType = typeOf[EntityCollection[_]]
  val rootCollType = typeOf[RootCollection[_]]
  val indexBaseType = typeOf[View[_, _]]
  val uniqueIndexType = typeOf[UniqueView[_, _]]

  def clear() = {
    resolvedMetas.clear()
    resolvedStructTypes.clear()
    unresolvedStructTypes.clear()
    delayResolvedStructTypes.clear()
  }

  def collectDBTypes(log: LoggingAdapter) = {
    clear()
    val nameMapping = mutable.Map[String, String]()
    def resolve(dbtype: DBType.Meta, symbolName: String) = {
      nameMapping.put(dbtype.name, symbolName) match {
        case Some(x) => throw new Error(s"db-name conflict: $symbolName and $x")
        case None =>
          resolvedMetas.put(dbtype.name, dbtype)
          resolvedStructTypes.put(symbolName, dbtype.name)
          unresolvedStructTypes.remove(symbolName)
      }
      log.debug("found dbtype: {} -> {}", dbtype.name, symbolName)
    }
    def resolveStruct(symbol: Symbol): Unit = {
      if (!resolvedStructTypes.contains(symbol.fullName)) {
        if (symbol.isClass) {
          val classSymbol = symbol.asClass
          if (classSymbol.isModuleClass) {
            resolveStruct(classSymbol.selfType.termSymbol)
          } else if (classSymbol.isAbstract) {
            if (classSymbol.isSealed) {
              val subs = classSymbol.knownDirectSubclasses
              subs foreach resolveStruct
              val dbtype = DBType.InterfaceMeta(fullName(symbol), isSealed = true, (subs map fullName).toSeq, Nil)
              resolve(dbtype, symbol.fullName)
            } else {
              val dbtype = DBType.InterfaceMeta(fullName(symbol), isSealed = false, Nil, Nil)
              resolve(dbtype, symbol.fullName)
            }
          } else if (classSymbol.isCaseClass) {
            val tpe = classSymbol.toType
            val ctor = tpe.decl(termNames.CONSTRUCTOR).asMethod
            val dbtype = DBType.StructMeta(fullName(symbol),
              ctor.paramLists(0) map { p =>
                DBType.ColumnMeta(localName(p), getTypeRef(p.typeSignature, p.fullName))
              }, Nil, isEntity = tpe <:< entityType)
            resolve(dbtype, symbol.fullName)
          } else
            throw new Error(s"expect case class: ${symbol.fullName}")
        } else if (symbol.isModule) {
          resolve(DBType.ValueMeta(fullName(symbol), Nil), symbol.fullName)
        } else
          throw new IllegalStateException()
      }
    }
    types(entityType.typeSymbol.fullName) foreach resolveStruct
    types(collType.typeSymbol.fullName) withFilter {
      !_.isAbstract
    } foreach { symbol =>
      val classSymbol = if (symbol.isModule) symbol.asModule.moduleClass.asClass else symbol.asClass
      val tpe = classSymbol.toType
      val valueType = getTypeRef(tpe.baseType(collType.typeSymbol).typeArgs(0), symbol.fullName)
      val builder = Seq.newBuilder[DBType.IndexMeta]
      tpe.members foreach { member =>
        if (member.owner == classSymbol && member.isMethod) {
          val returnType = member.asMethod.returnType
          if (returnType <:< indexBaseType) {
            val indexType = returnType.baseType(indexBaseType.typeSymbol)
            builder += DBType.IndexMeta(fullName(member),
              getTypeRef(indexType.typeArgs(0), member.fullName),
              returnType <:< uniqueIndexType, 0)
          }
        }
      }
      val dbtype = DBType.CollectionMeta(fullName(symbol), valueType,
        builder.result(), tpe <:< rootCollType, 0)
      resolve(dbtype, symbol.fullName)
    }
    types(typeOf[DataView[_, _]].typeSymbol.fullName) withFilter {
      !_.isAbstract
    } foreach { symbol =>
      val dbtype = DBType.DataViewMeta(fullName(symbol), 0)
      resolve(dbtype, symbol.fullName)
    }
    delayResolvedStructTypes foreach resolveStruct
    delayResolvedStructTypes.clear()
    if (unresolvedStructTypes.nonEmpty) {
      val sb = new StringBuilder
      sb.append("following types unresolved and used as dbtypes:\r\n")
      unresolvedStructTypes foreach {
        case (symbolName, refs) =>
          sb.append(s"\t'$symbolName' used by\r\n")
          refs foreach { ref =>
            sb.append(s"\t\t$ref\r\n")
          }
      }
      log.error(sb.toString())
      throw new Error("unresolve types used as dbtypes!")
    }
    this
  }

  def localName(s: Symbol) = Util.camelCase2seqStyle(s.name.toString)

  def fullName(s: Symbol) = Util.camelCase2seqStyle(s.name.toString)

  def getTypeRef(tpe: Type, ref: String): DBType.DBTypeRef = {
    if (tpe =:= stringType) DBType.String
    else if (tpe =:= definitions.IntTpe) DBType.Int
    else if (tpe =:= definitions.LongTpe) DBType.Long
    else if (tpe =:= definitions.DoubleTpe) DBType.Double
    else if (tpe =:= definitions.BooleanTpe) DBType.Boolean
    else if (tpe =:= datetimeType) DBType.DateTime
    else if (tpe =:= rawType) DBType.Raw
    else if (tpe.typeSymbol.fullName startsWith "scala.Tuple") {
      DBType.Tuple(tpe.typeArgs.map(getTypeRef(_, ref)))
    } else if (tpe <:< traversableType) {
      val tType = tpe.baseType(traversableType.typeSymbol)
      DBType.Coll(getTypeRef(tType.typeArgs(0), tpe.typeSymbol.fullName))
    } else if (tpe <:< refType) {
      val entityTpe = tpe.typeArgs.head
      if (entityTpe =:= entityType) {
        DBType.Ref(DBType.AbstractEntity)
      } else {
        DBType.Ref(getTypeRef(entityTpe, ref))
      }
    } else {
      resolvedStructTypes.get(tpe.typeSymbol.fullName) match {
        case Some(x) =>
          resolvedMetas(x) match {
            case m: DBType.StructMeta => DBType.Struct(x)
            case m: DBType.InterfaceMeta =>
              if (!m.isSealed) throw new Error(s"expect sealed trait or case class: ${tpe.typeSymbol.fullName} referenced by $ref")
              DBType.Struct(x)
            case _ => throw new IllegalStateException()
          }
        case None =>
          if (tpe <:< entityType)
            unresolvedStructTypes.getOrElseUpdate(tpe.typeSymbol.fullName, mutable.Set()).add(ref)
          else
            delayResolvedStructTypes.add(tpe.typeSymbol)
          DBType.Struct(fullName(tpe.typeSymbol))
      }
      DBType.Struct(fullName(tpe.typeSymbol))
    }
  }
}
