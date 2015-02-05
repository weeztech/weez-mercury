package com.weez.mercury

import scala.util.control.NonFatal

object Test {

  import akka.event._
  import common._

  import scala.reflect.runtime.universe._

  def main(args: Array[String]): Unit = {
    test0(args)
    import DBType._
    import debug.PackerDebug
    val value = EntityMeta(
      "collection-meta",
      List(), List(), true, false)

    val options = PackerDebug.ShowOptions()
    val options2 = PackerDebug.ShowOptions(offset = true, tpe = true, wrap = true)
    val buf = Packer.pack(value)
    println(PackerDebug.show(buf, options))
    Packer.unpack[EntityMeta](buf)
  }

  def test2() = {
    import product.Assistant
    val tree = reify {
      (v: Assistant, db: DBSessionQueryable) => {
        implicit val c = db
        v.staff().name
      }
    }.tree

    println(show(refact(tree, new Context {

      import scala.collection.mutable

      val scopes = mutable.Stack(mutable.Map[Name, Tree]())

      def subScope[T](f: => T): T = ???

      def addVal(name: Name, tree: Tree) = {
        scopes.top.put(name, tree)
      }

      def checkObserve(name: Name) = {
        scopes.top.get(name) match {
          case Some(x) => x.tpe
          case None => throw new IllegalStateException()
        }
      }
    })))
  }

  trait Context {
    def subScope[T](f: => T): T

    def addVal(name: Name, tree: Tree): Unit

    def checkObserve(name: Name): Unit
  }

  def refact(tree: Tree, c: Context): Tree = {
    tree match {
      case Function(params, body) =>
        val v = params.head
        body match {
          case Block(arr, ret) =>
            c.subScope {
              Block(arr map (refact(_, c)), refact(ret, c))
            }
          case x@ValDef(mods, name, tpt, rhs) =>
            c.addVal(name, tpt)
            ValDef(mods, name, tpt, refact(rhs, c))
          case Select(obj, member) =>
            val o = refact(obj, c)
            Select(o, member)
          case Apply(func, args) =>
            Apply(refact(func, c), args map (refact(_, c)))
          case x@Ident(name) =>
            c.checkObserve(name)
            x
          case If(cond, t, f) => If(refact(cond, c), refact(t, c), refact(f, c))
          case LabelDef(name, params, rhs) => LabelDef(name, params, refact(rhs, c))
          case _ => throw new Exception()
        }
    }
  }

  def test0(args: Array[String]) = {
    import scala.concurrent._
    import scala.concurrent.ExecutionContext.Implicits._
    val app = App.start(args)
    // showDB(app.serviceManager.database)
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
    val futu = call("common.SessionService.init") flatMap { m =>
      //call("debug.DBDebugService.listCollectionMetas", "prefix" -> "")
      call("debug.DBDebugService.listRootCollection", "sid" -> m.sid, "collectionName" -> CollectionMetaCollection.name)
    }
    futu.onComplete { result =>
      app.close()
      import scala.util._
      result match {
        case Success(x) => println(ModelObject.toJson(x).prettyPrint)
        case Failure(ex) => ex.printStackTrace()
      }
    }
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
