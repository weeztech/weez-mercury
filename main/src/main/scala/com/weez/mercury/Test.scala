package com.weez.mercury

import scala.util.control.NonFatal


object Test {
  def main(args: Array[String]): Unit = {
    test0(args)
  }

  def test0(args: Array[String]) = {
    import common._

    def showDB(db: Database) = {
      import akka.event.NoLogging
      import debug._
      val sb = new StringBuilder
      val dbSession = db.createSession()
      val trans = dbSession.newTransaction(NoLogging)
      val cursor = trans.newCursor()
      val options = PackerDebug.ShowOptions(hex = true)
      val options2 = PackerDebug.ShowOptions()
      cursor.seek(Array(Range.TYPE_MIN))
      while (cursor.isValid) {
        sb.append(PackerDebug.show(cursor.key(), options))
        sb.append(" -> ")
        sb.append(PackerDebug.show(cursor.value(), options2))
        sb.append("\r\n")
        cursor.next(1)
      }
      trans.close()
      dbSession.close()
      println(sb.toString())
    }

    import scala.concurrent._
    import scala.concurrent.ExecutionContext.Implicits._
    val app = App.start(args)
    showDB(app.database)
    val remoteCallManager = app.remoteCallManager
    val sessionManager = app.sessionManager
    val peer = sessionManager.ensurePeer(None)
    def call(api: String, args: (String, Any)*) = {
      try {
        remoteCallManager.postRequest(peer, "com.weez.mercury." + api, ModelObject(args: _*))
      } catch {
        case NonFatal(ex) => Future.failed(ex)
      }
    }
    val futu = call("product.SessionService.init") flatMap {
      case ModelResponse(m) =>
        //call("debug.DBDebugService.listCollectionMetas", "prefix" -> "")
        call("debug.DBDebugService.listRootCollection", "sid" -> m.sid, "collectionName" -> MetaCollection.name)
      case _ => ???
    }
    futu.onComplete { result =>
      app.close()
      import scala.util._
      result match {
        case Success(ModelResponse(x)) => println(JsonModel.to(x).prettyPrint)
        case Failure(ex) => ex.printStackTrace()
        case _ => throw new IllegalStateException()
      }
    }
  }

  import imports._

  object TestService extends RemoteService {
    val upload: SimpleCall = c => {
      import c._
      val id = acceptUpload(new Actor {
        def receive = {
          case UploadData(buf, uc) =>
            println(s"receive ${buf.length} bytes")
            sender ! UploadResume
          case UploadEnd(uc) =>
            println("upload end")
            uc.finish(ModelObject("finish" -> "done"))
            context.stop(self)
        }
      })
      modelWith("id" -> id)
    }
  }

}
