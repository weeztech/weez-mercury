package com.weez.mercury.product

import com.weez.mercury.imports._

/**
 * 提供会话、登录相关的功能。
 * @see [[com.weez.mercury.common.SessionManager]]
 */
object SessionService extends RemoteService {

  import java.security.MessageDigest
  import java.util.{Arrays => JArrays}

  def init: NoSessionCall = c => {
    import c._
    val session = app.sessionManager.createSession(peer)
    completeWith("sid" -> session.id)
  }

  /**
   * 使用用户名和密码登录。
   * ==request==
   * username: String 用户名 <br>
   * password: String 密码 <br>
   * ==response==
   * username: String 用户名 <br>
   * name: String 姓名 <br>
   * ==fail==
   * 用户名或密码错误
   */
  def login: QueryCall = c => {
    import c._
    UserCollection.byCode(request.username) match {
      case Some(user: User) =>
        if (!eqPassword(request.password, user.password))
          failWith("用户名或密码错误")
        session.login(user.id, user.name, user.staff().name)
        completeWith("username" -> user.code, "name" -> user.name)
      case None => failWith("用户名或密码错误")
    }
  }

  /**
   * 获取当前客户端所有登录的用户名和姓名。
   * ==request==
   * ==response==
   * logins: Seq <br>
   * - username: String 用户名 <br>
   * - name: String 姓名 <br>
   */
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
    completeWith("logins" -> builder.result().sortBy[String](_.name))
  }

  /**
   * 使用当前客户端已经登录过的身份登录当前会话，不需要用户名和密码。
   * ==request==
   * username: String 用户名 <br>
   * ==response==
   * username: String 用户名 <br>
   * name: String 姓名 <br>
   * ==fail==
   * 用户登录无效 <br>
   */
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

  /**
   * 获取服务器时间，并且防止会话过期，用作心跳。
   * ==response==
   * epoch: Long 时间 <br>
   * @see [[http://en.wikipedia.org/wiki/Unix_time]]
   */
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

  def eqPassword(pw1: String, pw2: Array[Byte]) = JArrays.equals(encodePassword(pw1), pw2)

  def eqPassword(pw1: Array[Byte], pw2: String): Boolean = eqPassword(pw2, pw1)
}


@packable
case class User(code: String, name: String, password: Array[Byte], staff: Ref[Staff], self: Ref[User]) extends Entity

object UserCollection extends RootCollection[User] {
  def name = "user"

  val byCode = defUniqueIndex("by-name", _.name)
  val byStaff = defUniqueIndex("by-staff", _.staff)
}
