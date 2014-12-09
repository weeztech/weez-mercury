package com.weez.mercury.common

import scala.concurrent.{ExecutionContext, Future}
import spray.json._

trait RemoteCall {
  def contextBinding: AnyRef

  def internalCall(factory: ContextFactory, req: JsValue): JsValue
}

abstract class ServiceCall[T: ContextBinding, I, O] extends RemoteCall {
  type Context = T

  val jsonRequest: RootJsonFormat[I]
  val jsonResponse: RootJsonFormat[O]

  def call(c: Context, req: I): O

  def contextBinding = implicitly[ContextBinding[T]]

  def internalCall(factory: ContextFactory, req: JsValue): JsValue = {
    val c = contextBinding.createContext(factory)
    call(c, req.convertTo[I](jsonRequest)).toJson(jsonResponse)
  }
}





