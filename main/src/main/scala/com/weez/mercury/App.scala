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
    app
  }

  import common._

  class ApplicationImpl(val system: ActorSystem, val config: Config) extends Application with common.ServiceManager {
    val devmode = config.getBoolean("devmode")
    val types = ClassFinder.collectTypes(system.log).map(tp => tp._1 -> tp._2.toSeq).toMap

    def actorFactory = system

    def app = this

    def serviceManager = this

    val sessionManager = new SessionManager(this, config)

    val remoteCalls = {
      val mirror = ru.runtimeMirror(this.getClass.getClassLoader)
      val builder = Map.newBuilder[String, RemoteCall]
      def collectCalls(classType: ru.Type, instance: Any) = {
        val instanceMirror = mirror.reflect(instance)
        classType.members foreach { member =>
          if (member.isPublic && member.isMethod) {
            val method = member.asMethod
            if (method.paramLists.isEmpty) {
              val tpe = method.returnType.baseType(ru.typeOf[Function1[_, _]].typeSymbol)
              if (!(tpe =:= ru.NoType)) {
                val paramType = tpe.typeArgs(0)
                if (ru.typeOf[ContextImpl] <:< paramType) {
                  builder += method.fullName -> RemoteCall(
                    instanceMirror.reflectMethod(method)().asInstanceOf[ContextImpl => Unit],
                    paramType <:< ru.typeOf[SessionState],
                    paramType <:< ru.typeOf[DBSessionQueryable],
                    paramType <:< ru.typeOf[DBSessionUpdatable])
                }
              }
            }
          }
        }
      }
      types("com.weez.mercury.common.RemoteService") withFilter {
        !_.isAbstract
      } foreach { symbol =>
        var instance: Any = null
        if (symbol.isModule) {
          collectCalls(symbol.typeSignature, mirror.reflectModule(symbol.asModule).instance)
        } else {
          val classSymbol = symbol.asClass
          val classType = classSymbol.toType
          val ctorSymbol = classType.member(ru.termNames.CONSTRUCTOR).asMethod
          if (ctorSymbol.paramLists.flatten.nonEmpty)
            throw new IllegalStateException(s"expect no arguments or abstract: ${classSymbol.fullName}")
          val ctorMirror = mirror.reflectClass(classSymbol).reflectConstructor(ctorSymbol)
          collectCalls(classType, ctorMirror())
        }
      }
      builder.result()
    }

    override def close() = {
      system.shutdown()
      super.close()
    }
  }

}
