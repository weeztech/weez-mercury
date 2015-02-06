package com.weez.mercury.product

import com.github.nscala_time.time.Imports._
import com.weez.mercury.imports._

object DataService extends RemoteService {
  def availableAssistants: QueryCall = c => {
    import c._
    val items = AssistantCollection.byStaffName().map { a =>
      ModelObject("id" -> a.id,
        "name" -> a.staff().name,
        "price" -> a.price,
        "description" -> a.description)
    }
    completeWith("items" -> items)
  }

  def availableRooms: QueryCall = c => {
    import c._
    val items = RoomCollection.byTitle().map { r =>
      ModelObject("id" -> r.id,
        "title" -> r.title,
        "price" -> r.price,
        "description" -> r.description)
    }
    completeWith("items" -> items)
  }

  def availableDevices: QueryCall = c => {
    import c._
    val keyword: String = request.keywords
    val items = ProductModelCollection.byTitle().filter(_.title.indexOf(keyword) >= 0)
      .drop(request.start).take(request.count).map { pm =>
      ModelObject("id" -> pm.id,
        "title" -> pm.code,
        "title" -> pm.title,
        "description" -> pm.description)
    }
    completeWith("items" -> items)
  }
}

@packable
case class ProductModel(code: String,
                        title: String,
                        description: String) extends Entity

object ProductModelCollection extends RootCollection[ProductModel] {
  def name = "product-model"

  val byCode = defUniqueIndex("by-code", _.code)
  val byTitle = defUniqueIndex("by-Title", _.title)
}

@packable
case class Product(code: String,
                   title: String,
                   description: String,
                   price: Double) extends Entity

object ProductCollection extends RootCollection[Product] {
  def name = "product"

  val byCode = defUniqueIndex("by-code", _.code)
  val byTitle = defUniqueIndex("by-code", _.title)
}

@packable
case class Assistant(price: Double,
                     staff: Ref[Staff],
                     description: String) extends Entity

object AssistantCollection extends RootCollection[Assistant] {
  def name = "assistant"

  def extendFrom = StaffCollection

  val byStaffName = defUniqueIndex("by-staff-name", _ => {
    ???
    "???"
  })
}

@packable
case class Customer(code: String,
                    title: String) extends Entity

object CustomerCollection extends RootCollection[Customer] {
  def name = "customer"

  val byCode = defUniqueIndex("by-code", _.code)
}

case class SaleOrder(code: String,
                     time: DateTime,
                     customer: Ref[Customer],
                     ctime: DateTime) extends Entity {
  lazy val rooms = new SaleOrderRoomItems(this)
}

object SaleOrder {

  @packable
  case class RoomItem(saleOrder: Ref[SaleOrder], seqID: Int, room: Ref[Room], startTime: DateTime, endTime: DateTime) extends Entity

  implicit val packer = Packer.caseClass(SaleOrder.apply _)
}

class SaleOrderRoomItems(owner: SaleOrder) extends SubCollection[SaleOrder, SaleOrder.RoomItem](owner) {
  def name = "sale-order-room-items"

  lazy val bySeqID = defUniqueIndex("bySeqID", _.seqID)
  lazy val byRoom = defUniqueIndex("byRoom", _.room)
}

object SaleOrderCollection extends RootCollection[SaleOrder] {
  def name = "sale-order"

  val byCode = defUniqueIndex("by-code", _.code)
}

object RoomItemCollection extends RootCollection[SaleOrder.RoomItem] {
  def name = "sale-order-room-items"

  //val bySeqID = defUniqueIndex[(Long, Int)]("bySeqID", v => (v.saleOrder.refID, v.seqID))
  //val byRoomID = defUniqueIndex[(Long, Long)]("byRoom", v => (v.saleOrder.refID, v.room.refID))
}

@packable
case class Room(code: String, title: String, price: Double, description: String) extends Entity

object RoomCollection extends RootCollection[Room] {
  def name = "room"

  val byTitle = defUniqueIndex("by-code", _.title)
}
