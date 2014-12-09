package wiz

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.io.{IO, Tcp}
import spray.can.Http
import spray.routing.HttpServiceActor

object HttpServer {
  def create(host: String, port: Int)(implicit system: ActorSystem) = {
    val actor = system.actorOf(Props(classOf[ServerActor], host, port), "http")
    new HttpServer(actor)
  }

  class ServerActor(host: String, port: Int) extends HttpServiceActor {
    override def preStart = {
      implicit val system = context.system
      IO(Http) ! Http.Bind(self, host, port)
    }

    def receive = {
      case _: Http.Bound => context.become(route)
      case Tcp.CommandFailed(_: Http.Bind) => context.stop(self)
    }

    def route: Receive = runRoute {
      path("ping") {
        get {
          complete("PONG")
        }
      }
    }
  }

}

class HttpServer(actor: ActorRef)
