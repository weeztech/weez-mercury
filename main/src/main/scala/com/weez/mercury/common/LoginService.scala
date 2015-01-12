package com.weez.mercury.common

import java.util.Arrays
import java.security.MessageDigest

object LoginService extends RemoteService {
  def login: QueryCall = c => {
    import c._
    UserCollection.byCode(request.userName) match {
      case Some(user: User) =>
        if (!eqPassword(request.password, user.password))
          failWith("用户名或密码错误")
        session.login(user.id, user.name, user.staff().name)
        completeWith("username" -> user.code, "name" -> user.name)
      case None => failWith("用户名或密码错误")
    }
  }

  def getLogins: SimpleCall = c => {
    import c._
    val builder = Seq.newBuilder[ModelObject]
    val set = scala.collection.mutable.Set[String]()
    sessionsByPeer() foreach { s =>
      s.loginState match {
        case Some(x) if !set.contains(x.username) =>
          set.add(x.username)
          builder += ModelObject(
            "username" -> x.username,
            "name" -> x.name
          )
        case _ =>
      }
    }
    completeWith("logins" -> builder.result.sortBy[String](_.name))
  }

  def quickLogin: SimpleCall = c => {
    import c._
    val username: String = request.username
    sessionsByPeer() collectFirst {
      case s if s.loginState.exists(_.username == username) =>
        s.loginState.get
    } match {
      case Some(x) =>
        session.login(x.userId, x.username, x.name)
        completeWith("username" -> x.username, "name" -> x.name)
      case None => failWith("用户登录无效")
    }
  }

  def getTime: SimpleCall = c => {
    import c._
    completeWith("epoch" -> System.currentTimeMillis())
  }

  private val digest = new ThreadLocal[MessageDigest]() {
    override def initialValue = MessageDigest.getInstance("MD5")
  }

  def encodePassword(password: String) = {
    digest.get().digest(password.getBytes("UTF-8"))
  }

  def eqPassword(pw1: String, pw2: Array[Byte]) = Arrays.equals(encodePassword(pw1), pw2)

  def eqPassword(pw1: Array[Byte], pw2: String): Boolean = eqPassword(pw2, pw1)
}

case class User(id: Long, code: String, name: String, password: Array[Byte], staff: Ref[Staff])

object User extends DBObjectType[User] {
  def nameInDB = "user"

  def name = column[String]("name")

  def password = column[Array[Byte]]("password")

  def staff = column[Ref[Staff]]("staff")

  implicit val packer = Packer(User.apply _)
}

object UserCollection extends RootCollection[User] {
  def byCode = defUniqueIndex("by-name", User.name)
}

case class Staff(code: String, name: String)

object Staff extends DBObjectType[Staff] {
  def nameInDB = "staff"

  def code = column[String]("code")

  def name = column[String]("name")

  implicit val packer = Packer(Staff.apply _)
}

object StaffCollection extends RootCollection[Staff] {
  def byCode = defUniqueIndex("by-code", Staff.name)
}