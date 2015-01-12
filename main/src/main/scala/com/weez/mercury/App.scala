package com.weez.mercury

import akka.actor._
import com.weez.mercury.common._

object App {
  def main(args: Array[String]): Unit = {
    val system = ActorSystem("mercury")
    val serviceManager = new ServiceManager(system)
    HttpServer.create(serviceManager)(system)
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
