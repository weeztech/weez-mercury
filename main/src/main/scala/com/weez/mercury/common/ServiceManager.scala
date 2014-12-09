package com.weez.mercury.common

import scala.language.existentials
import scala.concurrent.{Promise, ExecutionContext, Future}
import scala.concurrent.duration._
import scala.language.implicitConversions
import spray.json._
import akka.actor.{Props, ActorSystem}

class ServiceManager(system: ActorSystem) {
  val serviceCalls = Map[String, RemoteCall](
    LoginService
  )

  protected[common] val workers = system.actorOf(Props(classOf[WorkerActor]), "workers")

  private implicit def call2tuple(call: RemoteCall): (String, RemoteCall) = {
    call.getClass.getName -> call
  }

  def call(clazz: String, req: JsValue): Future[JsValue] = {
    implicit val timeout = 1.minutes
    serviceCalls.get(clazz) match {
      case Some(sc) =>
        val p = Promise[JsValue]
        workers ! RequestTask(sc, req, p)
        p.future
      case None =>
        Future.failed(new UnsupportedOperationException)
    }
  }
}

