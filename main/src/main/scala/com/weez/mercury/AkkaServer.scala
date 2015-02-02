package com.weez.mercury

object AkkaServer {

  import akka.actor._
  import common._
  import scala.util.control.NonFatal

  class ServerActor(app: Application) extends Actor with ActorLogging {
    def withPeer(f: String => Unit) = {
      sender().path.address.host match {
        case Some(x) =>
          try {
            f(x)
          } catch {
            case NonFatal(ex) => sender() ! Status.Failure(ex)
          }
        case None => log.warning("local request from: {}", sender().path.address)
      }
    }

    def receive = {
      case Connect() =>
        withPeer { peer =>
          app.sessionManager.createPeer(Some(peer))
          sender ! Connected()
        }
      case Request(api, r) =>
        withPeer { peer =>
          import scala.util._
          import context.dispatcher
          val remote = sender()
          app.serviceManager.postRequest(api, ModelObject(r: _*)).onComplete {
            case Success(x) => remote ! Response(x.underlaying.toArray)
            case Failure(ex) => remote ! Status.Failure(ex)
          }
        }
    }
  }

  case class Connect()

  case class Connected()

  case class Request(api: String, req: Array[(String, Any)])

  case class Response(req: Array[(String, Any)])

}

