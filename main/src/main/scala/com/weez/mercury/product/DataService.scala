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
      ModelObject("id" -> r.value.id,
        "title" -> r.value.title,
        "price" -> r.value.price,
        "description" -> r.value.description)
    }
    complete(model("items" -> items))
  }

  def availableDevices: QueryCall = c => {
    import c._
    val keyword: String = request.keywords
    val items = ProductCollection.byTitle().filter(_.value.title.indexOf(keyword) >= 0)
      .drop(request.start).take(request.count).map { pm =>
      ModelObject("id" -> pm.value.id,
        "title" -> pm.value.code,
        "title" -> pm.value.title,
        "description" -> pm.value.description)
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

@packable
case class SingleProductInfo(serialNumber: String, prevBiz: Ref[Entity], nextBiz: Ref[Entity])

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
                          from: Ref[Warehouse],
                          totalValue: Int,
                          remark: String)

@packable
case class RentOutSingleProduct(datetime: DateTime,
                                serialNumber: String,
                                from: Ref[Warehouse],
                                prevBizRef: Ref[Entity],
                                totalValue: Int,
                                remark: String)

@packable
case class RentOutProductReturn(datetime: DateTime,
                                quantity: Int,
                                to: Ref[Warehouse],
                                totalValue: Int,
                                remark: String)

@packable
case class RentOutSingleProductReturn(datetime: DateTime,
                                      serialNumber: String,
                                      to: Ref[Warehouse],
                                      nextBizRef: Ref[Entity],
                                      totalValue: Int,
                                      remark: String)

@packable
case class RentOutProductDamage(datetime: DateTime,
                                quantity: Int,
                                totalValue: Int,
                                remark: String)

@packable
case class RentOutSingleProductDamage(datetime: DateTime,
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
                                   outSingles: Seq[RentOutSingleProduct],
                                   returns: Seq[RentOutProductReturn],
                                   returnSingles: Seq[RentOutSingleProductReturn],
                                   damages: Seq[RentOutProductDamage],
                                   damageSingles: Seq[RentOutSingleProductDamage],
                                   remark: String)

@packable
case class RentOutOrder(datetime: DateTime,
                        number: String,
                        customer: Ref[Customer],
                        createTime: DateTime,
                        state: Int,
                        rooms: Seq[RentOutRoomItem],
                        assistants: Seq[RentOutAssistantItem],
                        products: Seq[RentOutOrderProductItem]) extends Entity

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
    var result = Set.empty[(Int, DateTime)]
    if (o.state == Booking || o.state == Open) {
      result += o.state -> o.datetime
    }
    result
  }
  extractTo(ProductFlowDataBoard) { (o, db) =>
    val bizRef = o.newRef()
    var result = List.empty[ProductFlow]
    for (p <- o.products) {
      for (out <- p.outs) {
        result ::= ProductFlow(bizRef, out.datetime, out.from, o.customer, p.product, out.quantity, out.totalValue)
      }
      for (ret <- p.returns) {
        result ::= ProductFlow(bizRef, ret.datetime, o.customer, ret.to, p.product, ret.quantity, ret.totalValue)
      }
      for (out <- p.outSingles) {
        result ::= ProductFlow(bizRef, out.datetime, out.from, o.customer, p.product, 1, out.totalValue)
      }
      for (ret <- p.returnSingles) {
        result ::= ProductFlow(bizRef, ret.datetime, o.customer, ret.to, p.product, 1, ret.totalValue)
      }
    }
    result
  }
  extractTo(SingleProductFlowDataBoard) { (o, db) =>
    val bizRef = o.newRef()
    var result = List.empty[SingleProductFlow]
    for (p <- o.products) {
      for (out <- p.outSingles) {
        result ::= SingleProductFlow(bizRef, out.datetime, out.from, o.customer, p.product, out.serialNumber,
          out.totalValue, p.returnSingles.exists(_.serialNumber == out.serialNumber))
      }
      for (ret <- p.returnSingles) {
        result ::= SingleProductFlow(bizRef, ret.datetime, o.customer, ret.to, p.product, ret.serialNumber,
          ret.totalValue, ret.nextBizRef.isDefined)
      }
    }
    result
  }
}


