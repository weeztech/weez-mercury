package com.weez.mercury

import scala.reflect.runtime.{universe => ru}
import akka.actor._
import com.typesafe.config.Config

object App {
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
    val app = new ApplicationImpl(system, config)
    if (app.config.getBoolean("http.enable")) {
      system.actorOf(Props(classOf[HttpServer.ServerActor], app, config.getConfig("http")), "http")
    }
    if (config.getBoolean("akka.enable")) {
      system.actorOf(Props(classOf[AkkaServer.ServerActor], app), "akka")
    }
    app.start()
    app
  }

  import common._

  class ApplicationImpl(val system: ActorSystem, val config: Config) extends Application {
    val _devmode = config.getBoolean("devmode")
    val _types = ClassFinder.collectTypes(system.log).map(tp => tp._1 -> tp._2.toSeq).toMap
    val dbtypeCollector = new DBTypeCollector(types).collectDBTypes(system.log)

    def devmode = _devmode

    def types = _types

    val sessionManager = new SessionManager(this, config)

    val serviceManager = new ServiceManager(this, config)

    def start() = {
      dbtypeCollector.clear()
    }

    system.registerOnTermination {
      serviceManager.close()
      sessionManager.close()
    }

    override def close() = {
      system.shutdown()
    }
  }

}
