package com.weez.mercury

object HttpServer {

  import scala.concurrent.Promise
  import scala.util._
  import shapeless._
  import akka.actor._
  import akka.io._
  import spray.can.Http
  import spray.routing._
  import spray.http._
  import spray.json._
  import spray.util.LoggingContext
  import com.typesafe.config._
  import com.weez.mercury.common._

  implicit def exceptionHandler: ExceptionHandler = {
    import Directives._
    ExceptionHandler {
      case ex: ProcessException =>
        complete(JsObject(
          "error" -> JsString(ex.getMessage),
          "code" -> JsNumber(ex.err.code)).toString())
      case ex: ModelException =>
        ex.printStackTrace()
        complete(JsObject(
          "error" -> JsString(ErrorCode.InvalidRequest.message),
          "code" -> JsNumber(ErrorCode.InvalidRequest.code)).toString())
      case ex: Throwable =>
        ex.printStackTrace()
        complete(StatusCodes.InternalServerError)
    }
  }

  class ServerActor(serviceManager: ServiceManager, config: Config) extends HttpServiceActor {
    val host = config.getString("host")
    val port = config.getInt("port")

    override def preStart() = {
      implicit val system = context.system
      IO(Http) ! Http.Bind(self, host, port)
    }

    def receive = {
      case _: Http.Bound => context.become(route)
      case Tcp.CommandFailed(_: Http.Bind) => context.stop(self)
    }

    private val PEER_NAME = "peer"

    def route: Receive = runRoute {
      post {
        cookie(PEER_NAME) { peer =>
          path("init") {
            withSession(peer.content) { sid =>
              complete(JsObject("result" -> JsString(sid)).toString())
            }
          } ~
            path("service" / Rest) { api =>
              postRequest(peer.value, api)
            } ~
            path("resource" / Rest) { resourceid =>
              ???
            }
        }
      } ~
        get {
          path("hello") {
            complete("hello")
          } ~ {
            val staticRoot = config.getString("root")
            val Resource = "^resource:(.+[^/])/?$".r
            val File = "^file:(.+[^/])/?$".r
            def normalize(p: String) = {
              import java.io.File.separator
              if (separator != "/") p.replace("/", separator) else p
            }

            pathSingleSlash {
              withPeer { peer =>
                setCookie(HttpCookie(PEER_NAME, peer)) {
                  staticRoot match {
                    case Resource(x) => getFromResource(x + "/index.html")
                    case File(x) => getFromFile(normalize(x + "/index.html"))
                    case _ => throw new ConfigException.BadValue(config.origin(), "root", "unsupported type")
                  }
                }
              }
            } ~ {
              staticRoot match {
                case Resource(x) => getFromResourceDirectory(x)
                case File(x) => getFromDirectory(normalize(x))
                case _ => throw new ConfigException.BadValue(config.origin(), "root", "unsupported type")
              }
            }
          }
        }
    }

    def withPeer = new Directive[String :: HNil] {
      def happly(f: String :: HNil => Route) = ctx => {
        val sessionManager = serviceManager.sessionManager
        val peer = ctx.request.cookies.find(_.name == PEER_NAME) match {
          case Some(x) => sessionManager.createPeer(x.content)
          case None => sessionManager.createPeer("")
        }
        f(peer :: HNil)(ctx)
      }
    }

    def withSession(peer: String) = new Directive[String :: HNil] {
      def happly(f: String :: HNil => Route) = ctx => {
        val sessionManager = serviceManager.sessionManager
        val session = sessionManager.createSession(peer)
        f(session.id :: HNil)(ctx)
      }
    }

    def postRequest(peer: String, api: String)(implicit log: LoggingContext): Route = ctx => {
      import context.dispatcher
      val startTime = System.nanoTime
      val p = Promise[JsValue]()
      val json = ctx.request.entity.asString.parseJson.asJsObject()
      val sid = json.fields.get("sid") match {
        case Some(JsString(x)) => x
        case _ => ErrorCode.InvalidRequest.raise
      }
      val req = json.fields.get("request") match {
        case Some(x: JsObject) => ModelObject.parse(x)
        case _ => ErrorCode.InvalidRequest.raise
      }
      serviceManager.postRequest(peer, sid, api, req).onComplete {
        case Success(out) =>
          import akka.event.Logging._
          val costTime = (System.nanoTime - startTime) / 1000000 // ms
          log.log(InfoLevel, "remote call complete in {} ms - {}", costTime, api)
          complete(out.toString)(ctx)
        case Failure(ex) => exceptionHandler(ex)(ctx)
      }
    }
  }

}
