package com.weez.mercury

import java.security.SecureRandom

import org.parboiled.common.Base64

import scala.concurrent.Promise
import scala.util._
import akka.actor.{ActorRef, ActorSystem, Props}
import akka.io.{IO, Tcp}
import spray.can.Http
import spray.routing.HttpServiceActor
import spray.http._
import spray.json._

import com.weez.mercury.common._

object HttpServer {
  def create(serviceManager: ActorRef)(implicit system: ActorSystem) = {
    val actor = system.actorOf(Props(classOf[ServerActor], serviceManager), "http")
    new HttpServer(actor)
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

    private val secureRandom = {
      val sr = SecureRandom.getInstance("NativePRNG", "SUN")
      sr.setSeed(System.currentTimeMillis())
      sr
    }
    private val base64 = Base64.rfc2045()
    private var sessionSeed = 0
    private val CookieName_SID = "sid"

    private def newSessionId: String = {
      val arr = new Array[Byte](16)
      secureRandom.nextBytes(arr)
      sessionSeed += 1
      arr(12) = (sessionSeed >> 24).asInstanceOf[Byte]
      arr(13) = (sessionSeed >> 16).asInstanceOf[Byte]
      arr(14) = (sessionSeed >> 8).asInstanceOf[Byte]
      arr(15) = sessionSeed.asInstanceOf[Byte]
      base64.encodeToString(arr, false)
    }

    def route: Receive = runRoute {
      path("service" / Rest) { clazz =>
        post { ctx =>
          import common.ErrorCode._
          try {
            val in = ctx.request.entity.asString.parseJson.asJsObject()
            val p = Promise[JsValue]
            ctx.request.cookies.find(_.name == CookieName_SID) match {
              case Some(x: HttpCookie) =>
                import context.dispatcher
                serviceManager ! WebRequest(x.value, clazz, in, p)
                p.future.onComplete {
                  case Success(out) => ctx.complete(out.toString())
                  case Failure(ex: ProcessException) =>
                    ctx.complete(JsObject("error" -> JsNumber(ex.err.code)).toString)
                  case Failure(_) =>
                    ctx.complete(StatusCodes.InternalServerError)
                }
              case None => throw new IllegalArgumentException
            }
          } catch {
            case ex: Throwable => ctx.complete(JsObject("error" -> JsNumber(InvalidRequest.code)).toString)
          }
        }
      } ~
        pathSingleSlash {
          get {
            redirect("/index.html", StatusCodes.PermanentRedirect)
          }
        } ~
        get {
          path("index.html") {
            setCookie(HttpCookie(CookieName_SID, newSessionId)) {
              getFromResource("web/index.html")
            }
          } ~
            getFromResourceDirectory("web")
        }
    }

    case class ServiceRequest(path: String, args: Array[Any])

  }

}

class HttpSession

class HttpServer(actor: ActorRef)
