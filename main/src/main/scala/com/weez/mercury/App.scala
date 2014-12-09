package com.weez.mercury

import akka.actor._
import com.weez.mercury.common.{ServiceCommand, ServiceManager}

object App {
  def main(args: Array[String]): Unit = {
    implicit val system = ActorSystem("weez")
    val ref = system.actorOf(Props(classOf[ServiceManager]), "mercury")
    HttpServer.create(ref)
  }
}
