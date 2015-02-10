package com.weez.mercury.common

/**
 * 提供用户直接调用的业务操作接口。
 * HttpServer/AkkaServer通过RemoteCallManager调用这些接口。
 * @define pager [[com.weez.mercury.common.RemoteService.completeWithPager]]
 */
@collect
trait RemoteService {

  type QueryCallContext = Context with SessionState with DBSessionQueryable
  type PersistCallContext = Context with SessionState with DBSessionUpdatable
  type SimpleCallContext = Context with SessionState
  type NoSessionCall = Context => Unit
  type SimpleCall = SimpleCallContext => Unit
  type QueryCall = QueryCallContext => Unit
  type PersistCall = PersistCallContext => Unit

  def modelWith(fields: (String, Any)*)(implicit c: Context): Unit = {
    c.response match {
      case null => c.complete(ModelResponse(ModelObject(fields: _*)))
      case ModelResponse(x) => x.update(fields: _*)
      case _ => throw new IllegalStateException()
    }
  }

  def sendModel(model: ModelObject)(implicit c: Context): Unit = {
    c.complete(ModelResponse(model))
  }

  def sendModel(fields: (String, Any)*)(implicit c: Context): Unit = {
    c.complete(ModelResponse(ModelObject(fields: _*)))
  }

  def sendFile(path: String)(implicit c: Context) = {
    c.complete(FileResponse(path))
  }

  def sendResource(url: String)(implicit c: Context) = {
    c.complete(ResourceResponse(url))
  }

  def sendStream(actor: => akka.actor.Actor)(implicit c: Context) = {
    c.complete(StreamResponse(() => actor))
  }

  def failWith(message: String) = {
    throw new ProcessException(ErrorCode.Fail, message)
  }

  /**
   * 支持分页请求，并返回分页结果。
   * ==request==
   * start: Int 起始序号 <br>
   * count: Int 数量 <br>
   * ==response==
   * items: Seq <br>
   * hasMore: Boolean 是否有更多记录未读取 <br>
   */
  def pager[T](cur: Cursor[T])(implicit c: Context): Unit = {
    import c._
    if (request.hasProperty("start")) {
      val start: Int = request.start
      val count: Int = request.count
      var arr = cur.slice(start, start + count + 1).toSeq
      val hasMore = arr.length == count + 1
      if (hasMore) arr = arr.slice(0, count)
      modelWith("items" -> arr, "hasMore" -> hasMore)
    } else {
      modelWith("items" -> cur.toSeq, "hasMore" -> false)
    }
    cur.close()
  }

  def saveUploadFile[C](path: String, overwrite: Boolean = false)(f: C => Unit)(implicit c: Context, evidence: ContextType[C]): String = {
    import akka.actor._
    import akka.util.ByteString

    c.acceptUpload(new Actor {
      var offset = 0L
      var file: FileIO.File = null

      def receive = {
        case UploadData(buf, uc) =>
          if (file == null) {
            val p = FileIO.pathOf(path)
            if (!overwrite && p.exists) {
              uc.fail(new IllegalStateException("file exists"))
              context.stop(self)
            } else {
              file = p.openFile(create = true, write = true, truncate = true)
              write(buf, uc)
            }
          } else
            write(buf, uc)
        case UploadEnd(uc) =>
          uc.finishWith(f)
          context.stop(self)
      }

      def write(buf: ByteString, uc: UploadContext) = {
        import scala.util._
        import context.dispatcher
        val ref = sender()
        file.write(buf, offset).onComplete {
          case Success(x) =>
            offset = x
            ref ! UploadResume
          case Failure(ex) =>
            uc.fail(ex)
            context.stop(self)
        }
      }
    })
  }
}

