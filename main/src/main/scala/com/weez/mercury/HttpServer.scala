package com.weez.mercury

import java.security.SecureRandom
import org.parboiled.common.Base64

import scala.annotation.tailrec
import scala.util.{Failure, Success}
import akka.actor.{ActorRef, ActorSystem, Props}
import akka.io.{IO, Tcp}
import spray.can.Http
import spray.routing.HttpServiceActor
import spray.http.StatusCodes
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

    private val secureRandom = SecureRandom.getInstance("NativePRNG", "SUN")
    secureRandom.setSeed(System.currentTimeMillis())
    private val base64 = Base64.rfc2045()
    private val sessions = Map.empty[String, HttpSession]

    @tailrec
    private def newSessionId: String = {
      val arr = new Array[Byte](16)
      secureRandom.nextBytes(arr)
      val sid = base64.encodeToString(arr, false)
      if (sessions.contains(sid))
        newSessionId
      else
        sid
    }

    def route: Receive = runRoute {
      path("service" / Rest) { clazz =>
        post { ctx =>
          import context.dispatcher
          val in = ctx.request.entity.asString.parseJson.asJsObject

          serviceManager.call(clazz, in).onComplete {
            case Success(out) => ctx.complete(out.toString())
            case Failure(ex) => ctx.complete(StatusCodes.InternalServerError)
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

class HttpSession

class HttpServer(actor: ActorRef)
