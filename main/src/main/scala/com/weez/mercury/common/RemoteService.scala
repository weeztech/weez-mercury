package com.weez.mercury.common

import com.weez.mercury.collect

@collect
trait RemoteService {
  type SimpleCall = Context with SessionState => Unit
  type QueryCall = Context with SessionState with DBSessionQueryable => Unit
  type PersistCall = Context with SessionState with DBSessionUpdatable => Unit

  def completeWith(fields: (String, Any)*)(implicit c: Context) = {
    if (c.response == null)
      c.complete(ModelObject(fields: _*))
    else
      c.response.update(fields: _*)
  }

  def failWith(message: String) = {
    throw new ProcessException(ErrorCode.Fail, message)
  }
}

