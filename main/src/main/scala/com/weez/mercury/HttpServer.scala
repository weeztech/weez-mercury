package com.weez.mercury

object HttpServer {

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
  import common._

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

  class ServerActor(app: ServiceManager, config: Config) extends HttpServiceActor {
    val host = config.getString("host")
    val port = config.getInt("port")

    override def preStart() = {
      implicit val system = context.system
      IO(Http) ! Http.Bind(self, host, port)
    }

    def receive = {
      case _: Http.Bound => context.become(upload orElse route)
      case Tcp.CommandFailed(_: Http.Bind) => context.stop(self)
    }

    private val PEER_NAME = "peer"

    def upload: Receive = {
      case req@ChunkedRequestStart(HttpRequest(HttpMethods.POST, Uri(_, _, Uri.Path(path), _, _), _, _, _))
        if path.startsWith("/upload/") =>
        runRoute { ctx =>
          val id = path.substring("/upload/".length)
          val uploadContext = app.uploadManager.startReceive(id)
          val ref = context.actorOf(Props(new DecodeActor(sender(), uploadContext)), s"decoder-$id")
          ref ! req
          sender ! Http.RegisterChunkHandler(ref)
          val startTime = System.nanoTime
          import context.dispatcher
          uploadContext.promise.future.onComplete {
            case Success(out) =>
              import akka.event.Logging._
              val log = implicitly[LoggingContext]
              val costTime = (System.nanoTime - startTime) / 1000000 // ms
              log.log(DebugLevel, "upload complete in {} ms - {}", costTime, id)
              out match {
                case ModelResponse(x) => complete(JsonModel.to(x).toString())(ctx)
                case FileResponse(x) => (getFromFile(x): Route)(ctx)
                case ResourceResponse(x) => (getFromResource(x): Route)(ctx)
                case StreamResponse(x) => ???
              }
            case Failure(ex) => exceptionHandler(ex)(ctx)
          }
        } apply HttpRequest(uri = req.request.uri)
    }

    def route: Receive = runRoute {
      post {
        withPeer { peer =>
          path("service" / Rest) { api =>
            postRequest(peer, api)
          } ~
            path("resource" / Rest) { resourceid =>
              ???
            }
        }
      } ~
        get {
          val staticRoot = config.getString("root")
          val Resource = "^resource:(.+[^/])/?$".r
          val File = "^file:(.+[^/])/?$".r
          def normalize(p: String) = {
            import java.io.File.separator
            if (separator != "/") p.replace("/", separator) else p
          }

          pathSingleSlash {
            staticRoot match {
              case Resource(x) => getFromResource(x + "/index.html")
              case File(x) => getFromFile(normalize(x + "/index.html"))
              case _ => throw new ConfigException.BadValue(config.origin(), "root", "unsupported type")
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

    def withPeer = new Directive[String :: HNil] {
      def happly(f: String :: HNil => Route) = ctx => {
        val sessionManager = app.sessionManager
        val cookiePeer = ctx.request.cookies.find(_.name == PEER_NAME).map(_.content)
        val peer = sessionManager.ensurePeer(cookiePeer)
        if (cookiePeer.contains(peer))
          f(peer :: HNil)(ctx)
        else
          setCookie(HttpCookie(PEER_NAME, peer)) {
            f(peer :: HNil)
          }(ctx)
      }
    }

    def postRequest(peer: String, api: String)(implicit log: LoggingContext): Route = ctx => {
      import context.dispatcher
      val startTime = System.nanoTime
      val req = parseJson(ctx.request.entity.asString)
      app.remoteCallManager.postRequest(peer, api, req).onComplete {
        case Success(out) =>
          import akka.event.Logging._
          val costTime = (System.nanoTime - startTime) / 1000000 // ms
          log.log(DebugLevel, "remote call complete in {} ms - {}", costTime, api)
          out match {
            case ModelResponse(x) => complete(JsonModel.to(x).toString())(ctx)
            case FileResponse(x) => (getFromFile(x): Route)(ctx)
            case ResourceResponse(x) => (getFromResource(x): Route)(ctx)
            case StreamResponse(x) => ???
          }
        case Failure(ex) => exceptionHandler(ex)(ctx)
      }
    }

    def parseJson(s: String) = {
      import scala.util.control.NonFatal
      try {
        JsonModel.parse(s.parseJson.asJsObject())
      } catch {
        case NonFatal(ex) => ErrorCode.InvalidRequest.raise
      }
    }
  }


  class DecodeActor(tcp: ActorRef, uploadContext: UploadContextImpl) extends Actor with ActorLogging {

    import scala.concurrent.duration._
    import akka.util.ByteString

    val highWaterMark = 200 * 1024
    val `multipart/form-data` = ContentTypeRange(MediaTypes.`multipart/form-data`)
    val disposeTimeout = 5.seconds
    val bufferQueue = new BufferQueue()
    var suspend = false
    var decoder: Decoder = EmptyDecoder

    def push(x: Try[ByteString]): Boolean = {
      x match {
        case Success(x) =>
          if (bufferQueue.enqueue(x) > highWaterMark && !suspend) {
            tcp ! Tcp.SuspendReading
            suspend = true
          }
          true
        case Failure(ex) =>
          uploadContext.fail(ex)
          context.unwatch(uploadContext.receiver)
          context.unwatch(tcp)
          context.stop(uploadContext.receiver)
          context.stop(self)
          false
      }
    }

    override def preStart() = {
      context.watch(tcp)
      context.watch(uploadContext.receiver)
      self ! UploadResume
    }

    def receive = receiveData orElse waitingUploadResume

    def receiveData: Receive = {
      case x: ChunkedRequestStart =>
        x.request.header[HttpHeaders.`Content-Type`] match {
          case Some(h) if `multipart/form-data`.matches(h.contentType) =>
            decoder = new MultipartFormDataDecoder(h.contentType)
        }
        val buf = x.message.entity.data.toByteString
        if (buf.nonEmpty) push(decoder.update(buf))
      case x: MessageChunk =>
        val buf = x.data.toByteString
        if (buf.nonEmpty) push(decoder.update(buf))
      case x: ChunkedMessageEnd =>
        if (push(decoder.end())) {
          bufferQueue.end()
          context.unwatch(tcp)
          context.become(waitingUploadResume)
        }
      case Terminated(x) if x == tcp =>
        uploadContext.fail(ErrorCode.RequestTimeout.exception)
        context.unwatch(uploadContext.receiver)
        context.stop(uploadContext.receiver)
        context.stop(self)
    }

    def waitingUploadResume: Receive = {
      case UploadResume =>
        bufferQueue.dequeue { buf =>
          if (buf.isEmpty) {
            uploadContext.receiver ! UploadEnd(uploadContext)
            context.setReceiveTimeout(disposeTimeout)
            context.become(waitingDispose)
          } else {
            uploadContext.receiver ! UploadData(buf, uploadContext)
            if (bufferQueue.length < highWaterMark && suspend) {
              tcp ! Tcp.ResumeReading
              suspend = false
            }
          }
        }
      case Terminated(x) if x == uploadContext.receiver =>
        if (!uploadContext.disposed) {
          log.error(s"upload receiver crashed, from remote call: ${uploadContext.api}")
          uploadContext.fail(throw new IllegalStateException("upload receiver crashed"))
        }
        context.become(dropRequest)
    }

    def waitingDispose: Receive = {
      case ReceiveTimeout =>
        log.error(s"upload receiver not disposed in $disposeTimeout, from remote call: ${uploadContext.api}")
        if (!uploadContext.disposed)
          uploadContext.fail(throw new IllegalStateException("UploadContext not finish"))
        context.unwatch(uploadContext.receiver)
        context.stop(self)
      case Terminated(x) if x == uploadContext.receiver =>
        context.setReceiveTimeout(Duration.Undefined)
        if (!uploadContext.disposed) {
          log.error(s"UploadContext not finish, from remote call: ${uploadContext.api}")
          uploadContext.fail(throw new IllegalStateException("UploadContext not finish"))
        }
        context.stop(self)
    }

    def dropRequest: Receive = {
      case Terminated(x) if x == tcp =>
        context.stop(self)
      case _: ChunkedMessageEnd =>
        context.unwatch(tcp)
        context.stop(self)
      case _: HttpRequestPart => // drop
    }
  }

  class BufferQueue() {

    import akka.util.ByteString

    private val builder = ByteString.newBuilder
    private var receive: ByteString => Unit = null
    private var isEnd = false

    def length = builder.length

    def dequeue(f: ByteString => Unit) = {
      require(receive == null)
      if (isEnd) {
        f(ByteString.empty)
      } else if (builder.length > 0) {
        f(builder.result())
        builder.clear()
      } else
        receive = f
    }

    def enqueue(buf: ByteString): Int = {
      require(!isEnd)
      if (buf.nonEmpty) {
        if (receive != null) {
          receive(buf)
          receive = null
        } else {
          builder.append(buf)
        }
      }
      builder.length
    }

    def end(): Unit = {
      require(!isEnd)
      isEnd = true
      if (receive != null) {
        receive(ByteString.empty)
        receive = null
      }
    }
  }

  trait Decoder {

    import akka.util.ByteString

    def update(buf: ByteString): Try[ByteString]

    def end(): Try[ByteString]
  }

  object EmptyDecoder extends Decoder {

    import akka.util.ByteString

    @inline final def update(buf: ByteString) = Success(buf)

    @inline final def end() = Success(ByteString.empty)
  }

  class MultipartFormDataDecoder(contentType: ContentType) extends Decoder {

    import akka.util.ByteString
    import spray.http._
    import spray.httpx.unmarshalling._

    var buffer = ByteString.empty

    def update(buf: ByteString) = {
      buffer = buffer.concat(buf)
      Success(ByteString.empty)
    }

    def end() = {
      val entity = HttpEntity(contentType, HttpData(buffer))
      Deserializer.MultipartFormDataUnmarshaller(entity) match {
        case Right(x) =>
          if (x.fields.length != 1)
            Failure(ErrorCode.InvalidRequest.exception)
          else
            Success(x.fields.head.entity.data.toByteString)
        case Left(ex) =>
          Failure(new IllegalArgumentException(ex.toString))
      }
    }
  }

}
