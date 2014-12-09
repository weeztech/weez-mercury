package com.weez.mercury.common

import scala.concurrent._
import spray.json._

trait RemoteCall {
  def dispatch(dispatcher: RemoteCallDispatcher, req: JsValue, promise: Promise[JsValue]): Unit
}

trait RemoteCallDispatcher {
  def postSimpleTask(f: SimpleContext => JsValue, promise: Promise[JsValue]): Unit

  def postQueryTask(f: QueryContext => JsValue, promise: Promise[JsValue]): Unit

  def postPersistTask(f: PersistContext => JsValue, promise: Promise[JsValue]): Unit
}

abstract class ServiceCall[T: ContextBinding, I, O] extends RemoteCall with DefaultJsonProtocol {
  type Context = T

  val jsonRequest: RootJsonFormat[I]
  val jsonResponse: RootJsonFormat[O]

  def call(c: Context, req: I): O

  def dispatch(dispatcher: RemoteCallDispatcher, req: JsValue, promise: Promise[JsValue]) = {
    val contextBinding = implicitly[ContextBinding[T]]
    contextBinding.dispatch(dispatcher, c => {
      call(c, req.convertTo[I](jsonRequest)).toJson(jsonResponse)
    }, promise)
  }
}

sealed trait ContextBinding[C] {
  def dispatch(dispatcher: RemoteCallDispatcher, f: C => JsValue, promise: Promise[JsValue]): Unit
}

object ContextBinding {

  implicit object SimpleContextBinding extends ContextBinding[SimpleContext] {
    def dispatch(dispatcher: RemoteCallDispatcher, f: SimpleContext => JsValue, promise: Promise[JsValue]) =
      dispatcher.postSimpleTask(f, promise)
  }

  implicit object QueryContextBinding extends ContextBinding[QueryContext] {
    def dispatch(dispatcher: RemoteCallDispatcher, f: QueryContext => JsValue, promise: Promise[JsValue]) =
      dispatcher.postQueryTask(f, promise)
  }

  implicit object PersistContextBinding extends ContextBinding[PersistContext] {
    def dispatch(dispatcher: RemoteCallDispatcher, f: PersistContext => JsValue, promise: Promise[JsValue]) =
      dispatcher.postPersistTask(f, promise)
  }

}





