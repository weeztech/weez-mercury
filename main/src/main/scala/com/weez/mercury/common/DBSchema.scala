package com.weez.mercury.common

import akka.event.LoggingAdapter

sealed trait DBType

object DBType {

  sealed trait DBTypeRef

  case class SimpleType private[DBType](typeCode: Int, name: String) extends DBType with DBTypeRef {
    override def toString = name
  }

  private object SimpleType

  val String = SimpleType(1, "String")
  val Int = SimpleType(2, "Int")
  val Long = SimpleType(3, "Long")
  val Double = SimpleType(4, "Double")
  val Boolean = SimpleType(5, "Boolean")
  val DateTime = SimpleType(6, "DateTime")
  val Raw = SimpleType(7, "Raw")

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

  implicit val typeRefPacker: Packer[DBTypeRef] = Packer.map[DBTypeRef, (Int, Array[Byte])]({
    case x: SimpleType => 1 -> Packer.pack(x.typeCode)
    case x: Tuple => 2 -> Packer.pack(x.parts)
    case x: Coll => 3 -> Packer.pack(x.element)
    case x: Ref => 4 -> typeRefPacker(x.tpe)
    case x: EntityRef => 10 -> Packer.pack(x.name)
    case x: EntityCollRef => 11 -> Packer.pack(x.name)
  }, x => {
    x._1 match {
      case 1 => fromTypeCode(Packer.unpack[Int](x._2))
      case 2 => Tuple(Packer.unpack[Seq[DBTypeRef]](x._2))
      case 3 => Coll(Packer.unpack[DBTypeRef](x._2))
      case 4 => Ref(typeRefPacker.unapply(x._2))
      case 10 => EntityRef(Packer.unpack[String](x._2))
      case 11 => EntityCollRef(Packer.unpack[String](x._2))
    }
  })

  case class Coll(element: DBTypeRef) extends DBType with DBTypeRef

  case class Tuple(parts: Seq[DBTypeRef]) extends DBType with DBTypeRef

  case class Ref(tpe: DBTypeRef) extends DBType with DBTypeRef

  case class EntityRef(name: String) extends DBTypeRef

  case class EntityCollRef(name: String) extends DBTypeRef

  sealed trait Named {
    def name: String
  }

  sealed trait Meta extends DBType with Named

  @packable
  case class ColumnMeta(name: String, tpe: DBTypeRef) extends Named

  @packable
  case class EntityMeta(name: String, columns: Seq[ColumnMeta], parents: Seq[String], isTopLevel: Boolean, isAbstract: Boolean) extends Meta with Entity

  @packable
  case class IndexMeta(name: String, key: DBTypeRef, unique: Boolean, prefix: Int) extends Named

  @packable
  case class CollectionMeta(name: String, valueType: DBTypeRef, indexes: Seq[IndexMeta], isRoot: Boolean, prefix: Int) extends Meta with Entity {
    def indexPrefixOf(name: String) = indexes.find(_.name == name).get.prefix
  }

}

object EntityMetaCollection extends RootCollection[DBType.EntityMeta] {
  def name = "entity-meta-collection"

  val byName = defUniqueIndex("by-name", _.name)
  val prefix = 1
}

object CollectionMetaCollection extends RootCollection[DBType.CollectionMeta] {
  def name = "collection-meta-collection"

  val byName = defUniqueIndex("by-name", _.name)
  val prefix = 2
}

object DBMetas {

  import DBType._

  val entityMetaCollectionMeta = CollectionMeta(
    EntityMetaCollection.name,
    EntityRef("entity-meta"),
    IndexMeta("by-name", String, unique = true, 1) :: Nil,
    isRoot = true, EntityMetaCollection.prefix)
  val collectionMetaCollectionMeta = CollectionMeta(
    CollectionMetaCollection.name,
    EntityRef("entity-meta"),
    IndexMeta("by-name", String, unique = true, 2) :: Nil,
    isRoot = true, CollectionMetaCollection.prefix)

  val metas = (
    entityMetaCollectionMeta ::
      collectionMetaCollectionMeta ::
      Nil).map(x => x.name -> x).toMap
}

import scala.reflect.runtime.universe._

class DBTypeCollector(types: Map[String, Seq[Symbol]]) {

  import scala.collection.mutable
  import org.joda.time.DateTime

  val resolvedMetas = mutable.ArrayBuffer[DBType.Meta]()
  val resolvedEntityTypes = mutable.Map[String, String]()
  val unresolvedEntityTypes = mutable.Map[String, mutable.Set[String]]()
  val delayResolvedEntityTypes = mutable.Map[String, Type]()

  val stringType = typeOf[String]
  val datetimeType = typeOf[DateTime]
  val rawType = typeOf[Array[Byte]]
  val refType = typeOf[Ref[_]]
  val traversableType = typeOf[Traversable[_]]

  val entityType = typeOf[Entity]
  val collType = typeOf[EntityCollection[_]]
  val rootCollType = typeOf[RootCollection[_]]
  val indexBaseType = typeOf[IndexBase[_, _]]
  val uniqueIndexType = typeOf[UniqueIndex[_, _]]

  def clear() = {
    resolvedMetas.clear()
    resolvedEntityTypes.clear()
    unresolvedEntityTypes.clear()
    delayResolvedEntityTypes.clear()
  }

  def collectDBTypes(log: LoggingAdapter) = {
    clear()
    val nameMapping = mutable.Map[String, String]()
    def resolve(dbtype: DBType.Meta, symbolName: String) = {
      nameMapping.put(dbtype.name, symbolName) match {
        case Some(x) => throw new Error(s"db-name conflict: $symbolName and $x")
        case None =>
          resolvedMetas.append(dbtype)
          resolvedEntityTypes.put(symbolName, dbtype.name)
          unresolvedEntityTypes.remove(symbolName)
      }
      log.debug("found dbtype: {} -> {}", dbtype.name, symbolName)
    }
    def resolveEntity(symbol: Symbol, tpe: Type, topLevel: Boolean) = {
      val dbtype =
        if (symbol.isAbstract) {
          DBType.EntityMeta(fullName(symbol), Nil, Nil, isTopLevel = topLevel, isAbstract = true)
        } else {
          val ctor = tpe.decl(termNames.CONSTRUCTOR).asMethod
          DBType.EntityMeta(fullName(symbol),
            ctor.paramLists(0) map { p =>
              DBType.ColumnMeta(localName(p), getTypeRef(p.typeSignature, p.fullName))
            }, Nil, isTopLevel = topLevel, isAbstract = false)
        }
      resolve(dbtype, symbol.fullName)
    }
    types(typeOf[Entity].typeSymbol.fullName) withFilter {
      !_.isAbstract
    } foreach { symbol =>
      resolveEntity(symbol, symbol.asType.toType, true)
    }
    types(typeOf[EntityCollection[_]].typeSymbol.fullName) withFilter {
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
    delayResolvedEntityTypes.values foreach { tpe =>
      resolveEntity(tpe.typeSymbol, tpe, false)
    }
    delayResolvedEntityTypes.clear()
    if (unresolvedEntityTypes.nonEmpty) {
      val sb = new StringBuilder
      sb.append("following types unresolved and used as dbtypes:\r\n")
      unresolvedEntityTypes foreach {
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
    } else if (tpe <:< entityType) {
      resolvedEntityTypes.get(tpe.typeSymbol.fullName) match {
        case Some(x) => DBType.EntityRef(x)
        case None =>
          unresolvedEntityTypes.getOrElseUpdate(tpe.typeSymbol.fullName, mutable.Set()).add(ref)
          DBType.EntityRef(fullName(tpe.typeSymbol))
      }
    } else if (tpe <:< refType) {
      DBType.Ref(getTypeRef(tpe.typeArgs.head, ref))
    } else if (tpe <:< collType) {
      DBType.EntityCollRef(fullName(tpe.typeSymbol))
    } else {
      delayResolvedEntityTypes.put(tpe.typeSymbol.fullName, tpe)
      DBType.EntityRef(fullName(tpe.typeSymbol))
    }
  }
}
