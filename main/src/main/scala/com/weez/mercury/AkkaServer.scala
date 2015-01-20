package com.weez.mercury

object AkkaServer {

  import akka.actor._
  import common._

  class ServerActor(serviceManager: ServiceManager) extends Actor with ActorLogging {
    def receive = {
      case Request(sid, api, r) =>
        sender().path.address.host match {
          case Some(x) =>
            serviceManager.postRequest(x, sid, api, new ModelObject(r))
          case None =>
            log.warning("local request from: {}", sender().path.address)
        }
    }
  }

  case class Request(sid: String, api: String, req: Map[String, Any])

}
