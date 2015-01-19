package com.weez.mercury.product

import com.github.nscala_time.time.Imports._
import com.weez.mercury.common._
import com.weez.mercury.product.SaleOrder.RoomItem

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
case class ProductModel(id: Long,
                        code: String,
                        title: String,
                        description: String) extends Entity

object ProductModelCollection extends RootCollection[ProductModel] {
  val name = "product-model"
  val byCode = defUniqueIndex("by-code", _.code)
  val byTitle = defUniqueIndex("by-Title", _.title)
}

@packable
case class Product(id: Long,
                   code: String,
                   title: String,
                   description: String,
                   price: Double) extends Entity

object ProductCollection extends RootCollection[Product] {
  val name = "product"
  val byCode = defUniqueIndex("by-code", _.code)
  val byTitle = defUniqueIndex("by-code", _.title)
}

@packable
case class Assistant(id: Long,
                     price: Double,
                     staff: Ref[Staff],
                     description: String) extends Entity

object AssistantCollection extends RootCollection[Assistant] {
  val name = "assistant"

  def extendFrom = StaffCollection

  val byStaffName = defUniqueIndex("by-staff-name", _ => {
    ???;
    "???"
  })
}

@packable
case class Customer(id: Long, code: String,
                    title: String) extends Entity

object CustomerCollection extends RootCollection[Customer] {
  val name = "customer"
  val byCode = defUniqueIndex("by-code", _.code)
}

case class SaleOrder(id: Long,
                     code: String,
                     time: DateTime,
                     customer: Ref[Customer],
                     ctime: DateTime) extends Entity {
  lazy val rooms = new SaleOrderRoomItems(this)
}

object SaleOrder {

  case class RoomItem(id: Long, saleOrder: Ref[SaleOrder], seqID: Int, room: Ref[Room], startTime: DateTime, endTime: DateTime) extends Entity

  implicit val packer2 = Packer(RoomItem)
  implicit val packer = Packer(SaleOrder.apply _)
}

class SaleOrderRoomItems(owner: Entity) extends SubCollection[RoomItem](owner) {
  override val name = "sale-order-room-items"

  lazy val bySeqID = defUniqueIndex("bySeqID", _.seqID)
  lazy val byRoom = defUniqueIndex("byRoom", _.room)
}

object SaleOrderCollection extends RootCollection[SaleOrder] {
  val name = "sale-order"
  val byCode = defUniqueIndex("by-code", _.code)
}

object RoomItemCollection extends RootCollection[RoomItem] {
  override val name = "sale-order-room-items"
  //val bySeqID = defUniqueIndex[(Long, Int)]("bySeqID", v => (v.saleOrder.refID, v.seqID))
  //val byRoomID = defUniqueIndex[(Long, Long)]("byRoom", v => (v.saleOrder.refID, v.room.refID))
}

@packable
case class Room(id: Long, title: String, price: Double, description: String) extends Entity

object RoomCollection extends RootCollection[Room] {
  val name = "room"
  val byTitle = defUniqueIndex("by-code", _.title)
}
