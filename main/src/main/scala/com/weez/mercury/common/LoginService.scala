package com.weez.mercury.common

import java.util.Arrays

import scala.concurrent._
import DB.driver.simple._

object LoginService extends RemoteService {
  def login: QueryCall = c => {
    import c._
    getUser(request.username).firstOption match {
      case Some((userId, code, name, pass)) =>
        val password = Staffs.makePassword(request.password)
        if (Arrays.equals(password, pass)) {
          val usid = session.login(userId, code, name)
          completeWith("success" -> 1)
        } else {
          completeWith("fail" -> 1)
        }
      case None =>
        completeWith("fail" -> 2)
    }
  }

  val getUser = Compiled((username: Column[String]) => {
    for (s <- Staffs if s.code === username) yield (s.id, s.code, s.name, s.password)
  })

  def getLogins: SimpleCall = c => {
    import c._
    val builder = Seq.newBuilder[ModelObject]
    val set = scala.collection.mutable.Set[Long]()
    sessionsByPeer() foreach { v =>
      v.loginState match {
        case Some(x) if !set.contains(x.userId) =>
          set.add(x.userId)
          builder += ModelObject(
            "name" -> x.name,
            "username" -> x.username
          )
        case _ =>
      }
    }
    completeWith("logins" -> builder.result.sortBy[String](_.name))
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
