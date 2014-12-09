package com.weez.mercury

import akka.actor.ActorSystem

import scala.concurrent._

object App {
  def main(args: Array[String]): Unit = {
    implicit val system = ActorSystem("weez-mercury")
    HttpServer.create("localhost", 8080)
  }
}
