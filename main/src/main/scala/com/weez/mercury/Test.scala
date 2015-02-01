package com.weez.mercury

object Test {

  import akka.event._
  import common._

  def main(args: Array[String]): Unit = {
    import scala.concurrent.ExecutionContext.Implicits._
    val app = App.start(args)
    showDB(app.serviceManager.database)
    val serviceManager = app.serviceManager
    val sessionManager = app.sessionManager
    val peer = "test"
    sessionManager.createPeer(Some(peer))
    val session = sessionManager.createSession(peer)

    def call(api: String, args: (String, Any)*) = {
      val api0 = "com.weez.mercury." + api
      val req = ModelObject(args: _*)
      serviceManager.postRequest(session, api0, req).onComplete { result =>
        sessionManager.returnAndUnlockSession(session)
        app.close()
        import scala.util._
        result match {
          case Success(x) => println(ModelObject.toJson(x).prettyPrint)
          case Failure(ex) => ex.printStackTrace()
        }
      }
    }

    //call("debug.DBDebugService.listCollectionMetas", "prefix" -> "")
    call("debug.DBDebugService.listRootCollection", "collectionName" -> CollectionMetaCollection.name)
  }

  def showDB(db: Database) = {
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
}
