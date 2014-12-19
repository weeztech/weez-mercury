package com.weez.mercury.common

import scala.concurrent._
import spray.json._

trait RemoteCall {
  def dispatch(req: JsValue): Task
}

abstract class ServiceCall[T: ContextBinding, I, O] extends RemoteCall with DefaultJsonProtocol {
  type Context = T

  val jsonRequest: RootJsonFormat[I]
  val jsonResponse: RootJsonFormat[O]

  def call(c: Context, req: I): O

  def dispatch(req: JsValue) = {
    val contextBinding = implicitly[ContextBinding[T]]
    contextBinding.dispatch(c => {
      call(c, req.convertTo[I](jsonRequest)).toJson(jsonResponse)
    })
  }
}

sealed trait ContextBinding[C] {
  def dispatch(f: C => JsValue): Task
}

object ContextBinding {

  implicit object SimpleContextBinding extends ContextBinding[Context] {
    def dispatch(f: Context => JsValue) = SimpleTask(f)
  }

  implicit object QueryContextBinding extends ContextBinding[Context with DBQuery] {
    def dispatch(f: Context with DBQuery => JsValue) = QueryTask(f)
  }

  implicit object PersistContextBinding extends ContextBinding[Context with DBPersist] {
    def dispatch(f: Context with DBPersist => JsValue) = PersistTask(f)
  }

}





