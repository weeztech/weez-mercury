package com.weez.mercury.product

import com.github.nscala_time.time.Imports._
import com.weez.mercury.common._

import scala.slick.jdbc.SetParameter

object DataService extends RemoteService {
  def availableAssistants: QueryCall = c => {
    import c._
    val items = sql"""SELECT a.id,s.name,a.price,a.description
             FROM biz_assistant a JOIN biz_staffs s ON s.id = a.id
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


case class Customer()

case class SaleOrder(code: String, time: DateTime, customer: Ref[Customer], rooms: KeyCollection[SaleOrder.RoomItem], ctime: DateTime)

object SaleOrder extends DBObjectType[SaleOrder] {
  def nameInDB = "sale-order"

  def code = column[String]("code")

  def dept = column[Dept]("dept")

  def access = column[KeyCollection[Dept]]("access")

  case class RoomItem(room: Ref[Room], startTime: DateTime, endTime: DateTime)

}

object SaleOrderCollection extends RootCollection[SaleOrder] {
  def byUsername = defUniqueIndex("by-username", User.name)
}

case class Room(code: String)
