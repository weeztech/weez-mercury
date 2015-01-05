package com.weez.mercury

import akka.actor._
import com.weez.mercury.common._

object App {
  def main(args: Array[String]): Unit = {
    implicit val system = ActorSystem("mercury")
    val serviceManager = new ServiceManager(system)
    HttpServer.create(serviceManager)
  }
}
