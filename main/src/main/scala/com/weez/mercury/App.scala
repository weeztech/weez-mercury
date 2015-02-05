package com.weez.mercury

object App {
  import akka.actor._
  import common._

  def main(args: Array[String]): Unit = {
    start(args)
  }

  def start(args: Array[String]) = {
    val system = ActorSystem("mercury")
    // use 'kill -15' (SIGTERM) or 'kill -2' (SIGINT) to terminate this application.
    // do NOT use 'kill -9' (SIGKILL), otherwise the shutdown hook will not work.
    // http://stackoverflow.com/questions/2541597/how-to-gracefully-handle-the-sigkill-signal-in-java
    Runtime.getRuntime.addShutdownHook(new Thread {
      override def run(): Unit = {
        system.shutdown()
      }
    })
    val config = system.settings.config.getConfig("weez-mercury")
    val app = new ServiceManager(system, config)
    if (app.config.getBoolean("http.enable")) {
      system.actorOf(Props(classOf[HttpServer.ServerActor], app, config.getConfig("http")), "http")
    }
    if (config.getBoolean("akka.enable")) {
      system.actorOf(Props(classOf[AkkaServer.ServerActor], app), "akka")
    }
    app.start()
    app
  }
}
