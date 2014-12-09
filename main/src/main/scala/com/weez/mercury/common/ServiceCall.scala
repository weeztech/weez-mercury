package com.weez.mercury.common

import scala.concurrent.{ExecutionContext, Future}
import spray.json._

trait RemoteCall {
  def dispatch(dispatcher: RemoteCallDispatcher, req: JsValue): Future[JsValue]
}

trait RemoteCallDispatcher {
  def postSimpleTask(f: SimpleContext => JsValue): Future[JsValue]

  def postQueryTask(f: QueryContext => JsValue): Future[JsValue]

  def postPersistTask(f: PersistContext => JsValue): Future[JsValue]
}

abstract class ServiceCall[T: ContextBinding, I, O] extends RemoteCall with DefaultJsonProtocol {
  type Context = T

  val jsonRequest: RootJsonFormat[I]
  val jsonResponse: RootJsonFormat[O]

  def call(c: Context, req: I): O

  def dispatch(dispatcher: RemoteCallDispatcher, req: JsValue) = {
    val contextBinding = implicitly[ContextBinding[T]]
    contextBinding.dispatch(dispatcher, c => {
      call(c, req.convertTo[I](jsonRequest)).toJson(jsonResponse)
    })
  }
}

sealed trait ContextBinding[C] {
  def dispatch(dispatcher: RemoteCallDispatcher, f: C => JsValue): Future[JsValue]
}

object ContextBinding {

  implicit object SimpleContextBinding extends ContextBinding[SimpleContext] {
    def dispatch(dispatcher: RemoteCallDispatcher, f: SimpleContext => JsValue) = dispatcher.postSimpleTask(f)
  }

  implicit object QueryContextBinding extends ContextBinding[QueryContext] {
    def dispatch(dispatcher: RemoteCallDispatcher, f: QueryContext => JsValue) = dispatcher.postQueryTask(f)
  }

  implicit object PersistContextBinding extends ContextBinding[PersistContext] {
    def dispatch(dispatcher: RemoteCallDispatcher, f: PersistContext => JsValue) = dispatcher.postPersistTask(f)
  }

}





