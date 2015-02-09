package com.weez.mercury

package object imports extends com.github.nscala_time.time.Imports {
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
  type SubCollection[O <: Entity, V <: Entity] = common.SubCollection[O, V]
  type Merger[V] = common.Merger[V]
  type collect = common.collect
  type ModelObject = common.ModelObject
  val ModelObject = common.ModelObject
  type Packer[T] = common.Packer[T]
  val Packer = common.Packer
  type UploadContext = common.UploadContext
  type UploadData = common.UploadData
  val UploadData = common.UploadData
  type UploadEnd = common.UploadEnd
  val UploadEnd = common.UploadEnd
  val UploadResume = common.UploadResume

  type Actor = akka.actor.Actor
  type ActorLogging = akka.actor.ActorLogging
  type ActorRef = akka.actor.ActorRef
  type Terminated = akka.actor.Terminated
  val Terminated = akka.actor.Terminated
  type ByteString = akka.util.ByteString
  val ByteString = akka.util.ByteString
  val ReceiveTimeout = akka.actor.ReceiveTimeout
}
