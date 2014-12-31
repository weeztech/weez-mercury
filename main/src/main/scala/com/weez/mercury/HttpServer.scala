package com.weez.mercury

import scala.concurrent.Promise
import scala.util._
import shapeless._
import akka.actor.{ActorRef, ActorSystem, Props}
import akka.io.{IO, Tcp}
import spray.can.Http
import spray.routing._
import spray.http._
import spray.json._

import com.weez.mercury.common._

object HttpServer {
  def create(serviceManager: ActorRef)(implicit system: ActorSystem) = {
    val actor = system.actorOf(Props(classOf[ServerActor], serviceManager), "http")
  }

  class ServerActor(serviceManager: ActorRef) extends HttpServiceActor {
    val config = context.system.settings.config.getConfig("weez-mercury.http")
    val host = config.getString("host")
    val port = config.getInt("port")

    override def preStart = {
      implicit val system = context.system
      IO(Http) ! Http.Bind(self, host, port)
    }

    def receive = {
      case _: Http.Bound => context.become(route)
      case Tcp.CommandFailed(_: Http.Bind) => context.stop(self)
    }

    private val CookieName_SID = "sid"

    def route: Receive = runRoute {
      post {
        cookie(CookieName_SID) { httpCookie =>
          path("service" / Rest) { clazz => ctx =>
            import context.dispatcher
            val p = Promise[JsValue]
            serviceManager ! WebRequest(httpCookie.value, clazz, ctx.request.entity.asString.parseJson.asJsObject(), p)
            p.future.onComplete {
              case Success(out) => ctx.complete(out.toString)
              case Failure(ex) => ctx.failWith(ex)
            }
          }
        } ~
          path("resource" / Rest) { resourceid =>
            ???
          }
      } ~
        get {
          pathSingleSlash {
            newSessionId { sid =>
              setCookie(HttpCookie(CookieName_SID, sid)) {
                if (webRoot.length == 0)
                  getFromResource("web/index.html")
                else
                  getFromFile(webRoot + "index.html")
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

    def newSessionId = new Directive1[String] {
      def happly(f: String :: HNil => Route) =
        ctx => {
          import context.dispatcher
          import akka.pattern.ask
          serviceManager.ask(CreateSession).onComplete {
            case Success(session: UserSession) =>
              f(session.id :: HNil)(ctx)
            case _ =>
              reject(ctx)
          }
        }
    }

    implicit def exceptionHandler = {
      ExceptionHandler {
        case ex: ProcessException =>
          complete(JsObject("error" -> JsNumber(ex.err.code)).toString)
        case ex: Throwable =>
          complete(StatusCodes.InternalServerError)
      }
    }


    val webRoot = {
      var s = System.getProperty("weez.web")
      if (s == null) s = ""
      if (!s.endsWith("/")) s += "/"
      s
    }
  }

}
