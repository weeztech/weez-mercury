package com.weez.mercury.product

import com.weez.mercury.DB
import com.weez.mercury.common._
import DB.driver.simple._

object DataService extends RemoteService {
  def availableAssistants: QueryCall = c => {
    import c._
    val q =
      for {
        a <- Assistants
        s <- Staffs
      } yield (a.id, s.name, a.price, a.description)
    val arr = q.run.map { tp =>
      ModelObject("id" -> tp._1,
        "name" -> tp._2,
        "price" -> tp._3,
        "description" -> tp._4)
    }
    completeWith("items" -> arr)
  }

  def availableRooms: QueryCall = c => {
    import c._
    val arr = Rooms.run.map { tp =>
      ModelObject("id" -> tp._1,
        "title" -> tp._2,
        "price" -> tp._3,
        "description" -> tp._4)
    }
    completeWith("items" -> arr)
  }

  val getProducts = Compiled((keywords: ConstColumn[String], start: ConstColumn[Long], count: ConstColumn[Long]) => {
    (for (p <- ProductModels if p.title like s"%$keywords%") yield p)
      .drop(start).take(count)
  })

  def availableDevices: QueryCall = c => {
    import c._
    val arr = getProducts(request.keywords, request.start, request.count).run.map { tp =>
      ModelObject("id" -> tp._1,
        "code" -> tp._2,
        "title" -> tp._3,
        "price" -> tp._4,
        "description" -> tp._5)
    }
    completeWith("items" -> arr)
  }
}

class Assistants(tag: Tag) extends Table[(Long, Double, String)](tag, "biz_assistants") {
  def id = column[Long]("id", O.PrimaryKey)

  def price = column[Double]("price")

  def description = column[String]("description")

  def staff = foreignKey("staff_fk", id, Staffs)(_.id)

  def * = (id, price, description)
}

object Assistants extends TableQuery(new Assistants(_))

class Rooms(tag: Tag) extends Table[(Long, String, Double, String)](tag, "biz_rooms") {
  def id = column[Long]("id", O.PrimaryKey)

  def title = column[String]("title")

  def price = column[Double]("price")

  def description = column[String]("description")

  def * = (id, title, price, description)
}

object Rooms extends TableQuery(new Rooms(_))

