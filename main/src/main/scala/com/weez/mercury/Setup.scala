package com.weez.mercury

import akka.actor.{Props, ActorSystem}
import com.typesafe.config.Config
import com.weez.mercury.common.{ServiceCommand, ServiceManager, Staffs}
import common.DB.driver.simple._

object Setup {
  def main(args: Array[String]): Unit = {
    implicit val system = ActorSystem("weez")
    val ref = system.actorOf(Props(classOf[ServiceManager]), "mercury")
    ref ! ServiceCommand.Setup
  }
}

