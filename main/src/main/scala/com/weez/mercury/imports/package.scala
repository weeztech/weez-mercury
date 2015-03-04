package com.weez.mercury

package object imports extends com.github.nscala_time.time.Imports {
  type Application = common.Application
  type RemoteService = common.RemoteService
  type Context = common.Context
  type SessionState = common.SessionState
  type Session = common.Session
  type DBSessionQueryable = common.DBSessionQueryable
  type DBSessionUpdatable = common.DBSessionUpdatable
  type Entity = common.Entity
  type Ref[+T <: Entity] = common.Ref[T]
  val RefSome = common.RefSome
  val RefEmpty = common.RefEmpty
  type EntityCollection[V <: Entity] = common.EntityCollection[V]
  type RootCollection[V <: Entity] = common.RootCollection[V]
  type DataView[K, V] = common.DataView[K, V]
  type DataBoard[D] = common.DataBoard[D]
  type CanMerge[Repr <: CanMerge[Repr]] = common.CanMerge[Repr]
  type NoMergeInDB = common.NoMergeInDB
  type FullTextSupport = common.FullTextSupport
  type View[K, V] = common.View[K, V]
  type UniqueView[K, V <: Entity] = common.UniqueView[K, V]
  type collect = common.collect
  type ModelObject = common.ModelObject
  val ModelObject = common.ModelObject
  type Packer[T] = common.Packer[T]
  val Packer = common.Packer
  type UploadContext = common.UploadContext
  type Response = common.Response
  type ModelResponse = common.ModelResponse
  val ModelResponse = common.ModelResponse
  type ResourceResponse = common.ResourceResponse
  val ResourceResponse = common.ResourceResponse
  type FileResponse = common.FileResponse
  val FileResponse = common.FileResponse
  type FutureResponse = common.FutureResponse
  val FutureResponse = common.FutureResponse
  type FailureResponse = common.FailureResponse
  val FailureResponse = common.FailureResponse

  type Actor = akka.actor.Actor
  type ActorLogging = akka.actor.ActorLogging
  type ActorRef = akka.actor.ActorRef
  type Terminated = akka.actor.Terminated
  val Terminated = akka.actor.Terminated
  type ByteString = akka.util.ByteString
  val ByteString = akka.util.ByteString
  val ReceiveTimeout = akka.actor.ReceiveTimeout
}
