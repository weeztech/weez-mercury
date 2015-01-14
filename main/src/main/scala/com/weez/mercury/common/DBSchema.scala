package com.weez.mercury.common

trait DBSchema {
  def newEntityID(): Long

  def getHostCollection(name: String): HostCollectionMeta
}

case class IndexMeta(id: Int,name: String)

case class HostCollectionMeta(id: Int, indexes: Seq[IndexMeta])

