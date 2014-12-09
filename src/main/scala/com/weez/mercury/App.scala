package wiz

import akka.actor.ActorSystem

object App {
  def main(args: Array[String]) = {
    implicit val system = ActorSystem("elemis")
    HttpServer.create("localhost", 8080)
    
  }
}
