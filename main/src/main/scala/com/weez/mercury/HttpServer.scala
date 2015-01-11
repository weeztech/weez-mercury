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

  case class WebRoot(tpe: String, path: String)

  class ServerActor(serviceManager: ServiceManager) extends HttpServiceActor {
    val config = context.system.settings.config.getConfig("weez-mercury.http")
    val host = config.getString("host")
    val port = config.getInt("port")

    val webRoot = {
      var s = config.getString("root")
      s = if (s.endsWith("/")) s.substring(0, s.length - 1) else s
      val i = s.indexOf(':')
      if (i < 0) throw new Exception("invalid config: weez-mercury.http.root")
      WebRoot(s.substring(0, i), s.substring(i + 1))
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
                  staticServe(Some("/index.html"))
                }
              }
            } ~
            staticServe()
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

    def staticServe(file: Option[String] = None): Route = {
      val path = file.map { s =>
        if (!s.startsWith("/")) "/" + s else s
      }
      webRoot.tpe match {
        case "resource" =>
          path match {
            case Some(x) => getFromResource(webRoot.path + file)
            case None => getFromResourceDirectory(webRoot.path)
          }
        case "file" =>
          def normalize(p: String) = {
            import java.io.File.separator
            if (separator != "/") p.replace("/", separator) else p
          }
          path match {
            case Some(x) => getFromFile(normalize(webRoot.path + file))
            case None => getFromDirectory(normalize(webRoot.path))
          }
        case _ => throw new Exception("invalid config: weez-mercury.http.root")
      }
    }
  }

}
