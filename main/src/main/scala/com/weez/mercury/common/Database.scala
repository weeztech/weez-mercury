package com.weez.mercury.common

object Database {

  case class Column[T](name: String)

}

trait DBSession {

}

trait Cursor[T] extends Iterator[T]

trait IndexBase[K, V] {
  def apply(start: K, end: K)(implicit db: DBSession): Cursor[V]
}

trait Index[K, V] extends IndexBase[K, V]

trait UniqueIndex[K, V] extends IndexBase[K, V] {
  def apply(key: K)(implicit db: DBSession): Option[V]
}

trait Ref[T] {
  def isEmpty: Boolean

  def apply()(implicit db: DBSession): T
}

trait DBObjectType[T] {

  import Database._

  def nameInDB: String

  def column[T](name: String) = Column[T](name)
}

trait KeyCollection[T] {
  def apply(id: Long)(implicit db: DBSession): Option[T]

  def defUniqueIndex[A](name: String, column: Database.Column[A]): UniqueIndex[A, T]
}

trait RootCollection[T] extends KeyCollection[T] {
  def apply(id: Long)(implicit db: DBSession): Option[T] = ???

  def defUniqueIndex[A](name: String, column: Database.Column[A]): UniqueIndex[A, T] = ???
}


object Example {

  case class Dept(name: String)

  case class User(name: String, code: String, dept: Ref[Dept], access: KeyCollection[Dept])

  object User extends DBObjectType[User] {
    def nameInDB = "user"

    def name = column[String]("name")

    def code = column[String]("code")

    def dept = column[Dept]("dept")

    def access = column[KeyCollection[Dept]]("access")
  }

  object UserCollection extends RootCollection[User] {
    def byUsername = defUniqueIndex("by-username", User.name)
  }
  
  def f(implicit db: DBSession) = {
    UserCollection.byUsername("abc") match {
      case Some(u) =>
        val dept = u.dept().name
      case None =>
    }
  }
}

// row
// username, password, access : Collection -> (pname -> P.id) Map MultiValueMap

// row
// P
// id,name
