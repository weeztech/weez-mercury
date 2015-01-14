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

case class ProductModel(id: Long,
                        code: String,
                        title: String,
                        description: String) extends Entity

object ProductModel extends DBObjectType[ProductModel] {
  def nameInDB = "product"

  val code = column[String]("code")

  val title = column[String]("title")

  val description = column[String]("description")

  implicit val packer = Packer(ProductModel.apply _)
}

object ProductModelCollection extends RootCollection[ProductModel] {
  val name = "product-model"
  val byCode = defUniqueIndex("by-code", _.code)
  val byTitle = defUniqueIndex("by-Title", _.title)
}


case class Product(id: Long,
                   code: String,
                   title: String,
                   description: String,
                   price: Double) extends Entity

object Product extends DBObjectType[Product] {
  def nameInDB = "product"

  val code = column[String]("code")
  val title = column[String]("title")
  val description = column[String]("description")

  implicit val packer = Packer(Product.apply _)
}

object ProductCollection extends RootCollection[Product] {
  val name = "product"
  val byCode = defUniqueIndex("by-code", _.code)
  val byTitle = defUniqueIndex("by-code", _.title)
}

case class Assistant(id: Long,
                     price: Double,
                     staff: Ref[Staff],
                     description: String) extends ExtendEntity[Staff]

object Assistant extends DBObjectType[Assistant] {
  def nameInDB = "assistant"

  val description = column[String]("description")
  val price = column[Double]("price")
  val staff = extend("staff", Staff)

  implicit val packer = Packer(Assistant.apply _)
}

object AssistantCollection extends RootCollection[Assistant] with ExtendCollection[Staff, Assistant] {
  val name = "assistant"

  def extendFrom = StaffCollection

  val byStaffName = defExtendIndex("by-staff-name", _.name)
}


case class Customer(id: Long, code: String,
                    title: String) extends Entity

object Customer extends DBObjectType[Customer] {
  def nameInDB = "customer"

  val code = column[String]("code")
  val title = column[String]("title")

  implicit val packer = Packer(Customer.apply _)
}

object CustomerCollection extends RootCollection[Customer] {
  val name = "customer"
  val byCode = defUniqueIndex("by-code", _.code)
}

case class SaleOrderRoomItems(host:SaleOrder) extends PartitionCollection[SaleOrder, RoomItem] {

  override def partitionOf = RoomItemCollection

  val bySeqID = defUniqueIndex(partitionOf.bySeqID)
  val byRoomID = defUniqueIndex(partitionOf.byRoomID)
}

case class SaleOrder(id: Long,
                     code: String,
                     time: DateTime,
                     customer: Ref[Customer],
                     ctime: DateTime) extends Entity{
  lazy val rooms = SaleOrderRoomItems(this)
  val roomsByRoomID = RoomItemCollection.byRoomID(this)
}

object SaleOrder extends DBObjectType[SaleOrder] {
  def nameInDB = "sale-order"

  val code = column[String]("code")

  val customer = column[Ref[Customer]]("code")

  val time = column[DateTime]("time")

  val ctime = column[DateTime]("ctime")

  val rooms = column[KeyCollection[RoomItem]]("rooms")

  case class RoomItem(id: Long, parentID: Long, seqID: Int, room: Ref[Room], startTime: DateTime, endTime: DateTime)
    extends SubEntity[SaleOrder]

  implicit val packer2 = Packer(RoomItem)
  implicit val packer = Packer(SaleOrder.apply _)
}

object SaleOrderCollection extends RootCollection[SaleOrder] {
  val name = "sale-order"
  val byCode = defUniqueIndex("by-code", _.code)
}

object RoomItemCollection extends RootCollection[RoomItem] with SubCollection[SaleOrder, RoomItem] {
  override val name = "sale-order-room-item"
  override val parent = SaleOrderCollection
  val bySeqID = defPartitionIndex[Int]("bySeqID", _.seqID)
  val byRoomID = defPartitionIndex[Long]("byRoom", _.room.refID)
}

case class Room(id: Long, title: String, price: Double, description: String) extends Entity

object Room extends DBObjectType[Room] {
  def nameInDB = "room"

  val title = column[String]("title")
  val price = column[Double]("price")
  val description = column[String]("description")

  implicit val packer = Packer(Room.apply _)
}

object RoomCollection extends RootCollection[Room] {
  val name = "room"
  val byTitle = defUniqueIndex("by-code", _.title)
}
