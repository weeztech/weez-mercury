package com.weez.mercury.product

import com.weez.mercury.common._
import DB.driver.simple._

case class AssistantSelectRequest()

case class AssistantSelectResponse(items: Seq[AssistantInfo])

case class AssistantInfo(id: Long, name: String, price: Double, description: String)

object AssistantSelectService extends ServiceCall[Context with DBQuery, AssistantSelectRequest, AssistantSelectResponse] {
  val jsonRequest = jsonFormat0(AssistantSelectRequest)
  implicit val jsonAssistantInfo = jsonFormat4(AssistantInfo)
  val jsonResponse = jsonFormat1(AssistantSelectResponse)

  def call(c: Context, req: AssistantSelectRequest): AssistantSelectResponse = {
    import c._
    val q =
      for {a <- Assistants
           s <- Staffs
      } yield (a.id, s.name, a.price, a.description)
    val arr = q.run.map { tp =>
      AssistantInfo(tp._1, tp._2, tp._3, tp._4)
    }
    AssistantSelectResponse(arr)
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


case class RoomSelectRequest()

case class RoomSelectResponse(items: Seq[RoomInfo])

case class RoomInfo(id: Long, title: String, price: Double, description: String)

object RoomSelectService extends ServiceCall[Context with DBQuery, RoomSelectRequest, RoomSelectResponse] {
  val jsonRequest = jsonFormat0(RoomSelectRequest)
  implicit val jsonAssistantInfo = jsonFormat4(RoomInfo)
  val jsonResponse = jsonFormat1(RoomSelectResponse)

  def call(c: Context, req: RoomSelectRequest): RoomSelectResponse = {
    import c._
    val arr = Rooms.run.map { tp =>
      RoomInfo(tp._1, tp._2, tp._3, tp._4)
    }
    RoomSelectResponse(arr)
  }
}

class Rooms(tag: Tag) extends Table[(Long, String, Double, String)](tag, "biz_rooms") {
  def id = column[Long]("id", O.PrimaryKey)

  def title = column[String]("title")

  def price = column[Double]("price")

  def description = column[String]("description")

  def * = (id, title, price, description)
}

object Rooms extends TableQuery(new Rooms(_))


case class DeviceSelectRequest(keywords: String, start: Int, count: Int)

case class DeviceSelectResponse(items: Seq[DeviceInfo])

case class DeviceInfo(id: Long, code: String, title: String, price: Double, description: String)

object DeviceSelectService extends ServiceCall[Context with DBQuery, DeviceSelectRequest, DeviceSelectResponse] {
  val jsonRequest = jsonFormat3(DeviceSelectRequest)
  implicit val jsonAssistantInfo = jsonFormat5(DeviceInfo)
  val jsonResponse = jsonFormat1(DeviceSelectResponse)

  val getProducts = Compiled((keywords: ConstColumn[String], start: ConstColumn[Long], count: ConstColumn[Long]) => {
    for (p <- ProductModels if p.title like s"%$keywords%") yield p

      .drop(start).take(count)
  })

  def call(c: Context, req: DeviceSelectRequest): DeviceSelectResponse = {
    import c._
    val arr = getProducts(req.start, req.count).run.map { tp =>
      DeviceInfo(tp._1, tp._2, tp._3, tp._4, tp._5)
    }
    DeviceSelectResponse(arr)
  }
}
