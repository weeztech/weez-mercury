package com.weez.mercury.common

/**
 * 提供用户直接调用的业务操作接口。
 * HttpServer/AkkaServer通过RemoteCallManager调用这些接口。
 * @define pager [[com.weez.mercury.common.RemoteService.completeWithPager]]
 */
@collect
trait RemoteService {

  type QueryCallContext = RemoteCallContext with SessionState with DBSessionQueryable
  type PersistCallContext = RemoteCallContext with SessionState with DBSessionUpdatable
  type SimpleCallContext = RemoteCallContext with SessionState
  type NoSessionCall = RemoteCallContext => Unit
  type SimpleCall = SimpleCallContext => Unit
  type QueryCall = QueryCallContext => Unit
  type PersistCall = PersistCallContext => Unit

  def model(fields: (String, Any)*): ModelResponse = ModelResponse(ModelObject(fields: _*))

  def model(m: ModelObject): ModelResponse = ModelResponse(m)

  def model(): ModelResponse = ModelResponse(ModelObject())

  def file(path: String): FileResponse = FileResponse(path)

  def file(path: FileIO.Path): FileResponse = FileResponse(path.toSystemPathString())

  def resource(path: String): ResourceResponse = ResourceResponse(path)

  def resource(path: FileIO.Path): ResourceResponse = ResourceResponse(path.toString())

  def fail(ex: Throwable): FailureResponse = FailureResponse(ex)

  def fail(message: String): FailureResponse = FailureResponse(throw new ProcessException(ErrorCode.Fail, message))

  /**
   * 支持分页请求，并返回分页结果。
   * ==request==
   * start: Int 起始序号 <br>
   * count: Int 数量 <br>
   * ==response==
   * items: Seq <br>
   * hasMore: Boolean 是否有更多记录未读取 <br>
   */
  def pager[T](cur: Cursor[T])(implicit c: RemoteCallContext): Unit = {
    import c._
    val m = {
      response match {
        case null =>
          val x = ModelObject()
          complete(ModelResponse(x))
          x
        case ModelResponse(x) => x
        case _ => throw new IllegalStateException("not model response")
      }
    }
    if (request.hasProperty("start")) {
      val start: Int = request.start
      val count: Int = request.count
      var arr = cur.slice(start, start + count + 1).toSeq
      val hasMore = arr.length == count + 1
      if (hasMore) arr = arr.slice(0, count)
      m.items = arr
      m.hasMore = hasMore
    } else {
      m.items = cur.toSeq
      m.hasMore = false
    }
    cur.close()
  }

  def saveUploadTempFile(f: (UploadContext, FileIO.Path) => Unit)(implicit c: Context with SessionState): String = {
    val id = saveUploadFile(c.tempDir, useIdAsFileName = true, overwrite = true)(f)
    c.session.synchronized {
      c.session.tempUploadFiles.add(c.tempDir / id)
    }
    id
  }

  def saveUploadFile(path: FileIO.Path,
                     useIdAsFileName: Boolean = false,
                     overwrite: Boolean = false)(
                      f: (UploadContext, FileIO.Path) => Unit)(implicit c: Context): String = {
    import scala.util._
    import scala.util.control.NonFatal

    c.acceptUpload { uc =>
      import akka.util.ByteString
      val filePath = if (useIdAsFileName) path / uc.id else path
      try {
        import uc.executor
        if (!overwrite && filePath.exists)
          throw new IllegalStateException("file exists")
        val file = filePath.openFile(create = true, write = true, truncate = true)
        var offset = 0L
        val content = uc.content
        content.read(new Pipe.Receiver[ByteString] {
          def onReceive(buf: ByteString) = {
            file.write(buf, offset).onComplete {
              case Success(off) =>
                offset = off
                content.resume()
              case Failure(ex) =>
                content.cancel(ex)
            }
          }

          def onEnd() = {
            file.close()
            f(uc, filePath)
          }

          def onError(ex: Throwable) = {
            file.close()
            try filePath.deleteIfExists() catch {
              case NonFatal(_) =>
            }
            uc.complete(fail(ex))
          }
        })
        content.resume()
      } catch {
        case NonFatal(ex) =>
          uc.complete(fail(ex))
      }
    }
  }
}
