package com.weez.mercury.common

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

object DBTypeEntityCollection extends RootCollection[DBType.Entity] {
  def name = "sys-types"

}
