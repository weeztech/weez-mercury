package com.weez.mercury.product

import com.github.nscala_time.time.Imports._
import com.weez.mercury.common._

object DataService extends RemoteService {
  def availableAssistants: QueryCall = c => {
    import c._
    val items = sql"""SELECT a.id,s.name,a.price,a.description
             FROM biz_assistants a JOIN biz_staffs s ON s.id = a.id
         """.as[ModelObject].list
    completeWith("items" -> items)
  }

  def availableRooms: QueryCall = c => {
    import c._
    val items = sql"SELECT id,title,price,description FROM biz_rooms".as[ModelObject].list
    completeWith("items" -> items)
  }

  def availableDevices: QueryCall = c => {
    import c._
    def getProducts(keywords: String, start: Int, count: Int) = {
      sqlp"""SELECT id,code,title,price,description FROM biz_product_models
           WHERE title LIKE CONCAT('%',$keywords,'%') LIMIT $start,$count""".as[ModelObject]
    }
    completeWith("items" -> getProducts(request.keywords, request.start, request.count).list)
  }
}

case class Product(code: String,
                   title: String,
                   description: String,
                   price: Double)

object Product extends DBObjectType[Product] {
  def nameInDB = "product"

  val code = column[String]("code")

  val title = column[String]("title")
}

object ProductCollection extends RootCollection[Product] {
  val byCode = defUniqueIndex("by-code", Product.code)
}



case class Assistant(code: String,
                     title: String)

object Assistant extends DBObjectType[Assistant] {
  def nameInDB = "assistant"

  val code = column[String]("code")

  val title = column[String]("title")
}

object AssistantCollection extends RootCollection[Assistant] {
  val byCode = defUniqueIndex("by-code", Assistant.code)
}


case class Customer(code: String,
                    title: String)

object Customer extends DBObjectType[Customer] {
  def nameInDB = "customer"

  val code = column[String]("code")

  val title = column[String]("title")
}

object CustomerCollection extends RootCollection[Customer] {
  val byCode = defUniqueIndex("by-code", Customer.code)
}

case class SaleOrder(code: String,
                     time: DateTime,
                     customer: Ref[Customer],
                     rooms: KeyCollection[SaleOrder.RoomItem],
                     ctime: DateTime)

object SaleOrder extends DBObjectType[SaleOrder] {
  def nameInDB = "sale-order"

  val code = column[String]("code")

  val customer = column[Ref[Customer]]("code")

  val time = column[DateTime]("time")

  val ctime = column[DateTime]("ctime")

  val rooms = column[KeyCollection[RoomItem]]("rooms")

  case class RoomItem(room: Ref[Room], startTime: DateTime, endTime: DateTime)

}

object SaleOrderCollection extends RootCollection[SaleOrder] {
  val byCode = defUniqueIndex("by-code", SaleOrder.code)
}

case class Room(code: String)
