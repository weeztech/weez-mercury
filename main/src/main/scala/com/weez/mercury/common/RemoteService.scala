package com.weez.mercury.common

@collect
trait RemoteService {

  type QueryCallContext = Context with SessionState with DBSessionQueryable
  type PersistCallContext = Context with SessionState with DBSessionUpdatable
  type SimpleCallContext = Context with SessionState
  type NoSessionCall = Context => Unit
  type SimpleCall = SimpleCallContext => Unit
  type QueryCall = QueryCallContext => Unit
  type PersistCall = PersistCallContext => Unit

  def completeWith(fields: (String, Any)*)(implicit c: Context) = {
    if (c.response == null)
      c.complete(ModelObject(fields: _*))
    else
      c.response.update(fields: _*)
  }

  def failWith(message: String) = {
    throw new ProcessException(ErrorCode.Fail, message)
  }

  def completeWithPager[T](cur: Cursor[T], keyprop: String)(implicit c: Context): Unit = {
    import c._
    if (request.hasProperty("start")) {
      val start: Int = request.start
      val count: Int = request.count
      var arr = cur.slice(start, start + count + 1).toSeq
      val hasMore = arr.length == count + 1
      if (hasMore) arr = arr.slice(0, count)
      completeWith("items" -> arr, "keyprop" -> keyprop, "hasMore" -> hasMore)
    } else {
      completeWith("items" -> cur.toSeq, "keyprop" -> keyprop, "hasMore" -> false)
    }
  }
}

