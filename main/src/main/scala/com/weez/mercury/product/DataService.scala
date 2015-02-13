package com.weez.mercury.product

import com.weez.mercury.imports._

object DataService extends RemoteService {

  import com.weez.mercury.common.DTOHelper._

  def availableAssistants: QueryCall = c => {
    import c._
    val items = AssistantCollection().map { a =>
      a.asMO { (mo, o) =>
        mo.name = o.staff().name
        mo.price = o.price
        mo.description = o.description
      }
    }
    complete(model("items" -> items))
  }

  def availableRooms: QueryCall = c => {
    import c._
    val items = RoomCollection.byTitle().map { r =>
      ModelObject("id" -> r.id,
        "title" -> r.title,
        "price" -> r.price,
        "description" -> r.description)
    }
    complete(model("items" -> items))
  }

  def availableDevices: QueryCall = c => {
    import c._
    val keyword: String = request.keywords
    val items = ProductCollection.byTitle().filter(_.title.indexOf(keyword) >= 0)
      .drop(request.start).take(request.count).map { pm =>
      ModelObject("id" -> pm.id,
        "title" -> pm.code,
        "title" -> pm.title,
        "description" -> pm.description)
    }
    complete(model("items" -> items))
  }
}

@packable
case class Product(code: String,
                   title: String,
                   description: String,
                   price: Int,
                   rentPrice: Int) extends Entity

object ProductCollection extends RootCollection[Product] {
  val byCode = defUniqueIndex("by-code", _.code)
  val byTitle = defUniqueIndex("by-title", _.title)
}

@packable
case class Room(code: String, title: String, price: Double, description: String) extends Entity

object RoomCollection extends RootCollection[Room] {
  val byCode = defUniqueIndex("by-code", _.title)
  val byTitle = defUniqueIndex("by-title", _.title)
}


@packable
case class Assistant(staff: Ref[Staff],
                     description: String,
                     price: Double) extends Entity


object AssistantCollection extends RootCollection[Assistant] {
  def name = "assistant"

  def extendFrom = StaffCollection
}

@packable
case class Customer(code: String,
                    title: String,
                    description: String) extends Entity

object CustomerCollection extends RootCollection[Customer] {
  val byCode = defUniqueIndex("by-code", _.code)
}


@packable
case class RentOutRoomItem(room: Ref[Room],
                           rantPrice: Int,
                           startTime: DateTime,
                           endTime: DateTime)

@packable
case class RentOutAssistantItem(assistant: Ref[Assistant],
                                rantPrice: Int,
                                startTime: DateTime,
                                endTime: DateTime)


@packable
case class RentOutProduct(datetime: DateTime,
                          quantity: Int,
                          serialNumber: String,
                          from: Ref[Warehouse],
                          totalValue: Int,
                          remark: String)

@packable
case class RentOutProductReturn(datetime: DateTime,
                                quantity: Int,
                                serialNumber: String,
                                to: Ref[Warehouse],
                                totalValue: Int,
                                remark: String)

@packable
case class RentOutProductDamage(datetime: DateTime,
                                quantity: Int,
                                serialNumber: String,
                                totalValue: Int,
                                remark: String)

@packable
case class RentOutOrderProductItem(product: Ref[Product],
                                   quantity: Int,
                                   rantPrice: Int,
                                   startTime: DateTime,
                                   endTime: DateTime,
                                   outs: Seq[RentOutProduct],
                                   returns: Seq[RentOutProductReturn],
                                   remark: String)

@packable
case class RentOutOrder(datetime: DateTime,
                        number: String,
                        customer: Ref[Customer],
                        createTime: DateTime,
                        state: Int,
                        rooms: Seq[RentOutRoomItem],
                        assistants: Seq[RentOutAssistantItem],
                        products: Seq[RentOutOrderProductItem]) extends Entity with ProductFlowBiz {
  def productFlows() = {
    val self = this
    products flatMap { item =>
      (item.outs map { o =>
        new ProductFlow {
          def bizRef = self

          def serialNumber = o.serialNumber

          def toStock = self.customer

          def datetime = o.datetime

          def product = item.product

          def fromStock = o.from

          def totalValue = o.totalValue

          def quantity = o.quantity
        }
      }) ++ (item.returns map { r =>
        new ProductFlow {
          def bizRef = self

          def serialNumber = r.serialNumber

          def toStock = r.to

          def datetime = r.datetime

          def product = item.product

          def fromStock = self.customer

          def totalValue = r.totalValue

          def quantity = r.quantity
        }
      })
    }
  }
}

object RentOutOrderState {
  final val Booking = 10
  final val Open = 20
  final val Closed = 30
}

object RentOutOrderCollection extends RootCollection[RentOutOrder] {
  def name = "rent-out-order"

  val byNumber = defUniqueIndex("by-number", _.number)
  val byNoneClosed = defIndexEx("by-none-closed") { (o, db) =>
    import RentOutOrderState._
    if (o.state == Booking || o.state == Open) {
      Set(Tuple2(o.state, o.datetime))
    } else {
      Set.empty[(Int, DateTime)]
    }
  }
}


