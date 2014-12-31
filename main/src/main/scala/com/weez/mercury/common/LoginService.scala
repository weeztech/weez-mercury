package com.weez.mercury.common

import java.util.Arrays

import spray.json.JsString

import scala.concurrent.{ExecutionContext, Future}
import DB.driver.simple._

object LoginService extends RemoteService {
  def login: QueryCall = c => {
    import c._

    val usid = in.fields.get("usid") match {
      case Some(JsString(x)) if x.length > 0 => x
      case _ => ErrorCode.InvalidUSID.raise
    }
    val userSession = ss.synchronized {
      ss.map.get("userSessions").map(_.asInstanceOf[Seq[UserSession]]) match {
        case Some(uss) =>
          uss.find(_.id == usid) match {
            case Some(us) => us
            case None => ErrorCode.InvalidUSID.raise
          }
        case None => ErrorCode.InvalidUSID.raise
      }
    }
    val req = in.fields.getOrElse("request", ErrorCode.InvalidRequest.raise)

    getUser(request.username).firstOption match {
      case Some((userId, pass)) =>
        val password = Staffs.makePassword(request.password)
        if (Arrays.equals(password, pass)) {
          val usid = c.changeUser(userId)
          Response(0, usid)
        } else {
          Response(1, "")
        }
      case None =>
        Response(2, "")
    }
  }

  case class Request(username: String, password: String)

  case class Response(result: Int, usid: String)

  val jsonRequest = jsonFormat2(Request)
  val jsonResponse = jsonFormat2(Response)

  val getUser = Compiled((username: Column[String]) => {
    for (s <- Staffs if s.code === username) yield (s.id, s.password)
  })

  def call(c: Context, req: Request): Response = {
    import java.util.Arrays
    import c._
    getUser(req.username).firstOption match {
      case Some((userId, pass)) =>
        val password = Staffs.makePassword(req.password)
        if (Arrays.equals(password, pass)) {
          val usid = c.changeUser(userId)
          Response(0, usid)
        } else {
          Response(1, "")
        }
      case None =>
        Response(2, "")
    }
  }
}

object QuickLoginService extends ServiceCall[Context] {

  case class Request(code: String)

  case class Response(result: Int, usid: String)

  val jsonRequest = jsonFormat1(Request)
  val jsonResponse = jsonFormat2(Response)

  def call(c: Context, req: Request): Response = {

  }
}

object LoginCheckService extends ServiceCall[Context] {

  case class Request()

  case class Response(logins: Seq[LoginInfo])

  case class LoginInfo(code: String, name: String)

  val jsonRequest = jsonFormat0(Request)
  val jsonResponse = jsonFormat1(Response)

  def call(c: Context, req: Request): Response = {
    val password = Staffs.makePassword(req.password)
    if (Arrays.equals(password, pass)) {
      val usid = c.changeUser(userId)
      Response(0, usid)
    } else {
      Response(1, "")
    }
  }
}

class Staffs(tag: Tag) extends Table[(Long, String, String, Array[Byte])](tag, "biz_staffs") {
  def id = column[Long]("id", O.PrimaryKey)

  def code = column[String]("code")

  def name = column[String]("name")

  def password = column[Array[Byte]]("password")

  def * = (id, code, name, password)
}

object Staffs extends TableQuery(new Staffs(_)) {
  def makePassword(password: String) = {
    import java.security.MessageDigest
    MessageDigest.getInstance("MD5").digest(password.getBytes)
  }
}
