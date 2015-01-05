package com.weez.mercury

import com.typesafe.config.ConfigFactory
import com.weez.mercury.common._

object Setup {
  def main(args: Array[String]): Unit = {
    import DB.driver.simple._
    val db = Database.forConfig("weez-mercury.database.writable", ConfigFactory.load(this.getClass.getClassLoader))
    db.withTransaction { implicit session =>
      val ddl = ServiceManager.tables.tail.foldLeft(ServiceManager.tables.head.ddl) { (ddl, t) =>
        ddl ++ t.ddl
      }

      ddl.dropStatements foreach { s =>
        session.withTransaction {
          try {
            session.withPreparedStatement(s)(_.execute)
          } catch {
            case ex: Throwable =>
              ex.printStackTrace
          }
        }
      }
      ddl.create

      Staffs +=(1L, "test", "test", Staffs.makePassword("test"))
    }
  }
}

