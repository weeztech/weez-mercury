package com.weez.mercury

object Imports {
  type Application = common.Application
  type RemoteService = common.RemoteService
  type Context = common.Context
  type SessionState = common.SessionState
  type DBSessionQueryable = common.DBSessionQueryable
  type DBSessionUpdatable = common.DBSessionUpdatable
  type Entity = common.Entity
  type Ref[+T <: Entity] = common.Ref[T]
  type EntityCollection[V <: Entity] = common.EntityCollection[V]
  type RootCollection[V <: Entity] = common.RootCollection[V]
  type DataView[K, V] = common.DataView[K, V]
  type IndexBase[K, V] = common.IndexBase[K, V]
  type Index[K, V <: Entity] = common.Index[K, V]
  type UniqueIndex[K, V <: Entity] = common.UniqueIndex[K, V]
  type Packer[T] = common.Packer[T]
  type collect = common.collect
}
