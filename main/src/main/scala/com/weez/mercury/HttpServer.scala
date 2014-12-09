package com.weez.mercury

import spray.http.StatusCodes

import scala.util.{Failure, Success}

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.io.{IO, Tcp}
import spray.can.Http
import spray.routing.HttpServiceActor
import spray.json._

import com.weez.mercury.common.ServiceManager

object HttpServer {
  def create(host: String, port: Int)(implicit system: ActorSystem) = {
    val actor = system.actorOf(Props(classOf[ServerActor], host, port), "http")
    new HttpServer(actor)
  }

  class ServerActor(host: String, port: Int) extends HttpServiceActor {
    val exeutionContext = context.system.dispatchers.lookup(
      context.system.settings.config.getString("weez-mercury.servicecall-dispatcher"))

    val serviceManager = new ServiceManager(context.system)

    override def preStart = {
      implicit val system = context.system
      IO(Http) ! Http.Bind(self, host, port)
    }

    def receive = {
      case _: Http.Bound => context.become(route)
      case Tcp.CommandFailed(_: Http.Bind) => context.stop(self)
    }

    def route: Receive = runRoute {
      path("service" / Rest) { clazz =>
        post { ctx =>
          import context.dispatcher
          val jsonAst = ctx.request.entity.asString.parseJson.asJsObject
          val req = jsonAst.fields("request")
          serviceManager.call(clazz, req).onComplete { result =>
            val ret =
              result match {
                case Success(resp) =>
                  JsObject("result" -> resp)
                case Failure(ex) =>
                  JsObject("error" -> JsString(ex.getMessage))
              }
            ctx.complete(ret.toString())
          }
        }
      } ~
        pathSingleSlash {
          get {
            redirect("/index.html", StatusCodes.PermanentRedirect)
          }
        } ~
        get {
          getFromResourceDirectory("web")
        }
    }

    case class ServiceRequest(path: String, args: Array[Any])

  }

}

class HttpServer(actor: ActorRef)
