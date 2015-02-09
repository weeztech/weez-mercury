package com.weez.mercury.product

import com.github.nscala_time.time.Imports._
import com.weez.mercury.imports._

import scala.reflect.internal.util.Statistics.Quantity

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
    val items = ProductCollection.byTitle().filter(_.title.indexOf(keyword) >= 0)
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
case class Room(code: String, title: String, price: Double, description: String) extends Entity

object RoomCollection extends RootCollection[Room] {
  def name = "room"

  val byTitle = defUniqueIndex("by-code", _.title)
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
                    title: String) extends Entity

object CustomerCollection extends RootCollection[Customer] {
  def name = "customer"

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
case class RentOutOrderProductItem(product: Ref[Product],
                                   quantity: Int,
                                   rantPrice: Int,
                                   startTime: DateTime,
                                   endTime: DateTime)

@packable
case class RentOutOrder(datetime: DateTime,
                        number: String,
                        customer: Ref[Customer],
                        createTime: DateTime,
                        rooms: Seq[RentOutRoomItem],
                        assistants: Seq[RentOutAssistantItem],
                        products: Seq[RentOutOrderProductItem]) extends Entity {
}

object RentOutOrderCollection extends RootCollection[RentOutOrder] {
  def name = "rent-out-order"

  val byNumber = defUniqueIndex("by-number", _.number)
}


