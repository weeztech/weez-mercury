package com.weez.mercury

object HttpServer {

  import scala.concurrent._
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
          val uploadContext = app.remoteCallManager.openUpload(id)
          val ref = context.actorOf(Props(new DecodeActor(sender(), uploadContext)), s"decoder-$id")
          ref ! req
          sender ! Http.RegisterChunkHandler(ref)
          processResponse(uploadContext.future, ctx, s"upload $id")
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
      val req = parseJson(ctx.request.entity.asString)
      val futu = app.remoteCallManager.postRequest(peer, api, req)
      processResponse(futu, ctx, s"remote call $api")
    }

    def parseJson(s: String) = {
      import scala.util.control.NonFatal
      try {
        JsonModel.parse(s.parseJson.asJsObject())
      } catch {
        case NonFatal(ex) => ErrorCode.InvalidRequest.raise
      }
    }


    def processResponse(futu: Future[InstantResponse], ctx: RequestContext, message: String) = {
      import akka.event.Logging._
      import context.dispatcher
      val startTime = System.nanoTime()
      futu.onComplete {
        case Success(resp) =>
          val log = implicitly[LoggingContext]
          val costTime = (System.nanoTime - startTime) / 1000000 // ms
          log.log(DebugLevel, "complete in {} ms - {}", costTime, message)
          resp match {
            case ModelResponse(x) => complete(JsonModel.to(x).toString())(ctx)
            case FileResponse(x) => (getFromFile(x): Route)(ctx)
            case ResourceResponse(x) => (getFromResource(x): Route)(ctx)
            case StreamResponse(x) => ???
            case FailureResponse(ex) => exceptionHandler(ex)(ctx)
          }
        case Failure(ex) => exceptionHandler(ex)(ctx)
      }
    }
  }


  import akka.util.ByteString

  class DecodeActor(tcp: ActorRef, uploadContext: UploadContextImpl) extends Actor with ActorLogging {

    import scala.concurrent.duration._

    val highWaterMark = 200 * 1024
    val `multipart/form-data` = ContentTypeRange(MediaTypes.`multipart/form-data`)
    val receiver = uploadContext.receiver
    var bufferQueue: AsyncQueue[ByteString, ByteString] = new BufferQueue(highWaterMark, tcp)

    var stateReceiving = false
    var stateUploading = false

    override def preStart() = {
      stateReceiving = true
      stateUploading = true
      context.watch(tcp)
      self ! UploadResume
    }

    def receive = {
      case x: ChunkedRequestStart if stateReceiving =>
        x.request.header[HttpHeaders.`Content-Type`] match {
          case Some(h) if `multipart/form-data`.matches(h.contentType) =>
            bufferQueue = bufferQueue >> new MultipartFormDataDecoder(h.contentType)
        }
        bufferQueue.enqueue(x.message.entity.data.toByteString)
      case x: MessageChunk if stateReceiving =>
        bufferQueue.enqueue(x.data.toByteString)
      case x: ChunkedMessageEnd if stateReceiving =>
        bufferQueue.end()
        context.unwatch(tcp)
        stateReceiving = false
      case UploadResume if stateUploading =>
        bufferQueue.dequeue {
          case QueueItem(x) =>
          //receiver ! UploadData(x)
          case QueueError(ex) =>
            //receiver ! UploadCancelled
            uploadContext.fail(ex)
            //context.become(afterUpload)
            stateUploading = false
          case QueueFinish =>
            //receiver ! UploadEnd
            //context.become(afterUpload)
            stateUploading = false
        }
      case Terminated(x) if x == tcp =>
        if (!uploadContext.isComplete)
          uploadContext.fail(ErrorCode.NetworkError.exception)
        if (receiver == null) {
          context.stop(self)
        } else {
          //receiver ! UploadCancelled
        }
      case Terminated(x) if x == receiver =>
        if (stateUploading) {
          log.error(s"upload receiver crashed, from remote call: ${uploadContext.api}")
          uploadContext.fail(throw new IllegalStateException("upload receiver crashed"))
          if (stateReceiving)
            context.become(dropRequest)
        }
      case x: UploadResult =>
        uploadContext.complete(x)
        if (stateReceiving)
          context.become(dropRequest)
        else
          context.stop(self)
    }

    def dropRequest: Receive = {
      case _: ChunkedMessageEnd =>
        context.unwatch(tcp)
        context.stop(self)
      case _: HttpRequestPart => // drop
    }
  }

  class BufferQueue(highWaterMark: Int, tcp: ActorRef) extends AsyncQueue.AbstractQueue[ByteString, ByteString] {
    private val builder = ByteString.newBuilder
    private var suspend = false
    private var size = 0

    protected def enqueue0(buf: ByteString) = {
      builder.append(buf)
      size += buf.length
      if (size > highWaterMark && !suspend) {
        suspend = true
        tcp ! Tcp.SuspendReading
      }
    }

    protected def dequeue0(): ByteString = {
      if (builder.length == 0) notReady()
      val buf = builder.result()
      builder.clear()
      size -= buf.length
      if (suspend && size < highWaterMark) {
        suspend = false
        tcp ! Tcp.ResumeReading
      }
      buf
    }
  }

  class MultipartFormDataDecoder(contentType: ContentType) extends AsyncQueue.AbstractQueue[ByteString, ByteString] {

    import spray.http._
    import spray.httpx.unmarshalling._

    private val builder = ByteString.newBuilder

    override def enqueue0(buf: ByteString) = {
      builder.append(buf)
    }

    override def dequeue0() = {
      if (!isEnd) notReady()
      val buf = builder.result()
      builder.clear()
      val entity = HttpEntity(contentType, HttpData(buf))
      Deserializer.MultipartFormDataUnmarshaller(entity) match {
        case Right(x) =>
          if (x.fields.length != 1) ErrorCode.InvalidRequest.raise
          x.fields.head.entity.data.toByteString
        case Left(ex) =>
          throw new IllegalArgumentException(ex.toString)
      }
    }
  }

}
