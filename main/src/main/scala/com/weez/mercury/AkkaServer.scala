package com.weez.mercury

object AkkaServer {

  import akka.actor._
  import common._

  class ServerActor(app: Application) extends Actor with ActorLogging {
    def withPeer(f: String => Unit) = {
      sender().path.address.host match {
        case Some(x) => x
        case None => log.warning("local request from: {}", sender().path.address)
      }
    }

    def receive = {
      case Connect() =>
        withPeer { peer =>
          app.sessionManager.createPeer(Some(peer))
          val session = app.sessionManager.createSession(peer)
          sender ! SessionCreated(session.id)
        }
      case Request(sid, api, r) =>
        withPeer { peer =>
          app.sessionManager.getAndLockSession(sid) match {
            case Some(session) =>
              import scala.util._
              import context.dispatcher
              val remote = sender()
              app.serviceManager.postRequest(session, api, ModelObject(r: _*)).onComplete { result =>
                app.sessionManager.returnAndUnlockSession(session)
                result match {
                  case Success(x) => remote ! Response(x.underlaying.toArray)
                  case Failure(ex) => remote ! Status.Failure(ex)
                }
              }
            case None => sender ! Status.Failure(new Exception("session not found"))
          }
        }
    }
  }

  case class Connect()

  case class SessionCreated(sid: String)

  case class Request(sid: String, api: String, req: Array[(String, Any)])

  case class Response(req: Array[(String, Any)])

}

