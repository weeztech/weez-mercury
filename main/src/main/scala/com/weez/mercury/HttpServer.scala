package com.weez.mercury

import akka.event.Logging.LogLevel

import scala.concurrent.Promise
import scala.util._
import shapeless._
import akka.actor.{ActorRef, ActorSystem, Props}
import akka.io.{IO, Tcp}
import spray.can.Http
import spray.routing._
import spray.http._
import spray.json._
import spray.util.LoggingContext

import com.weez.mercury.common._

object HttpServer {
  def create(serviceManager: ServiceManager)(implicit system: ActorSystem) = {
    system.actorOf(Props(classOf[ServerActor], serviceManager), "http")
  }

  implicit def exceptionHandler = {
    import Directives._
    ExceptionHandler {
      case ex: ProcessException =>
        complete(JsObject(
          "error" -> JsString(ex.getMessage),
          "code" -> JsNumber(ex.err.code)).toString)
      case ex: ModelException =>
        ex.printStackTrace
        complete(JsObject(
          "error" -> JsString(ErrorCode.InvalidRequest.message),
          "code" -> JsNumber(ErrorCode.InvalidRequest.code)).toString)
      case ex: Throwable =>
        ex.printStackTrace
        complete(StatusCodes.InternalServerError)
    }
  }

  class ServerActor(serviceManager: ServiceManager) extends HttpServiceActor {
    val config = context.system.settings.config.getConfig("weez-mercury.http")
    val host = config.getString("host")
    val port = config.getInt("port")

    val webRoot = {
      val s = config.getString("root")
      if (s.endsWith("/")) s.substring(0, s.length - 1) else s
    }

    override def preStart = {
      implicit val system = context.system
      IO(Http) ! Http.Bind(self, host, port)
    }

    def receive = {
      case _: Http.Bound => context.become(route)
      case Tcp.CommandFailed(_: Http.Bind) => context.stop(self)
    }

    private val PEER_NAME = "peer"

    def route: Receive = runRoute {
      post {
        cookie(PEER_NAME) { peer =>
          path("init") {
            withSession(peer.content) { sid =>
              complete(JsObject("result" -> JsString(sid)).toString)
            }
          } ~
            path("service" / Rest) { api =>
              postRequest(peer.value, api)
            } ~
            path("resource" / Rest) { resourceid =>
              ???
            }
        }
      } ~
        get {
          path("hello") {
            complete("hello")
          } ~
            pathSingleSlash {
              withPeer { peer =>
                setCookie(HttpCookie(PEER_NAME, peer)) {
                  if (webRoot.length == 0)
                    getFromResource("web/index.html")
                  else
                    getFromFile(webRoot + "/index.html")
                }
              }
            } ~ {
            if (webRoot.length == 0)
              getFromResourceDirectory("web")
            else
              getFromDirectory(webRoot)
          }
        }
    }

    def withPeer = new Directive[String :: HNil] {
      def happly(f: String :: HNil => Route) = ctx => {
        val sessionManager = serviceManager.sessionManager
        val peer = ctx.request.cookies.find(_.name == PEER_NAME) match {
          case Some(x) => sessionManager.createPeer(x.content)
          case None => sessionManager.createPeer("")
        }
        f(peer :: HNil)(ctx)
      }
    }

    def withSession(peer: String) = new Directive[String :: HNil] {
      def happly(f: String :: HNil => Route) = ctx => {
        val sessionManager = serviceManager.sessionManager
        val session = sessionManager.createSession(peer)
        f(session.id :: HNil)(ctx)
      }
    }

    def postRequest(peer: String, api: String)(implicit log: LoggingContext): Route = ctx => {
      import context.dispatcher
      val startTime = System.nanoTime
      val p = Promise[JsValue]
      serviceManager.postRequest(peer, api, ctx.request.entity.asString.parseJson.asJsObject(), p)
      p.future.onComplete {
        case Success(out) =>
          import akka.event.Logging._
          val costTime = (System.nanoTime - startTime) / 1000000 // ms
          log.log(InfoLevel, "remote call complete in {} ms - {}", costTime, api)
          complete(out.toString)(ctx)
        case Failure(ex) => exceptionHandler(ex)(ctx)
      }
    }
  }

}
