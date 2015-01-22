package com.weez.mercury

import akka.actor._
import com.typesafe.config.ConfigFactory
import com.weez.mercury.common._

object App {

  import scala.reflect.runtime.{universe => ru}

  val system = ActorSystem("mercury")
  val config = system.settings.config.getConfig("weez-mercury")
  val devmode = config.getBoolean("devmode")
  val types: Map[String, Seq[ru.Symbol]] = ClassFinder.collectTypes(system.log).map(tp => tp._1 -> tp._2.toSeq).toMap

  def main(args: Array[String]): Unit = {
    val serviceManager = new ServiceManager(system, config)
    if (config.getBoolean("http.enable")) {
      system.actorOf(Props(classOf[HttpServer.ServerActor], serviceManager, config.getConfig("http")), "http")
    }
    if (config.getBoolean("akka.enable")) {
      system.actorOf(Props(classOf[AkkaServer.ServerActor], serviceManager), "akka")
    }
    system.registerOnTermination {
      serviceManager.close()
    }
    // use 'kill -15' (SIGTERM) or 'kill -2' (SIGINT) to terminate this application.
    // do NOT use 'kill -9' (SIGKILL), otherwise the shutdown hook will not work.
    // http://stackoverflow.com/questions/2541597/how-to-gracefully-handle-the-sigkill-signal-in-java
    Runtime.getRuntime.addShutdownHook(new Thread {
      override def run(): Unit = {
        system.shutdown()
      }
    })
  }
}
