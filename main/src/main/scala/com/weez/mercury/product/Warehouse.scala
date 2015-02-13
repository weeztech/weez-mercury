package com.weez.mercury.product

import com.weez.mercury.common.RefSome
import com.weez.mercury.imports._
import com.weez.mercury.common.DTOHelper._

import scala.util.Random

/**
 * 仓库
 */
@packable
final case class Warehouse(code: String,
                           title: String,
                           description: String) extends Entity

object WarehouseCollection extends RootCollection[Warehouse] {

  val byCode = defUniqueIndex("by-code", _.code)
}

@packable
case class Provider(code: String,
                    title: String, description: String) extends Entity

object ProviderCollection extends RootCollection[Provider] {
  val byCode = defUniqueIndex("by-code", _.code)
}

case class ProductFlow(bizRefID: Long,
                       datetime: DateTime,
                       fromStockID: Long,
                       toStockID: Long,
                       productID: Long,
                       serialNumber: String,
                       quantity: Int,
                       totalValue: Int)

object ProductFlowDataBoard extends DataBoard[ProductFlow]


/**
 * 库存明细账－主键部分
 * @param stockID 库ID，可能是仓库，或部门
 * @param productID 物品ID
 * @param datetime 发生时间
 * @param peerStockID 对应库ID
 * @param bizRefID 业务参考ID，各种单据的ID
 */
@packable
final case class StockAccountKey(stockID: Long, productID: Long, datetime: DateTime, peerStockID: Long, bizRefID: Long) {
  def revert() = StockAccountKey(peerStockID, productID, datetime, stockID, bizRefID)
}

/**
 * 库存明晰帐－值部分
 * @param quantity 数量
 * @param totalValue 价值
 */
@packable
final case class StockAccountValue(quantity: Int,
                                   totalValue: Int) extends CanMerge[StockAccountValue] {

  @inline def +(that: StockAccountValue) = StockAccountValue(this.quantity + that.quantity, this.totalValue + that.totalValue)

  @inline def -(that: StockAccountValue) = StockAccountValue(this.quantity - that.quantity, this.totalValue - that.totalValue)

  @inline def isEmpty = quantity == 0 && totalValue == 0

  @inline def neg() = StockAccountValue(-quantity, -totalValue)
}


object StockAccount extends DataView[StockAccountKey, StockAccountValue] {

  extractFrom(ProductFlowDataBoard) { (o, db) =>
    val k = StockAccountKey(o.toStockID, o.productID, o.datetime, o.fromStockID, o.bizRefID)
    val v = StockAccountValue(o.quantity, o.totalValue)
    Map(k -> v, k.revert() -> v.neg())
  }
}

@packable
final case class StockSummaryAccountKey(stockID: Long, productID: Long, datetime: DateTime)

@packable
final case class StockSummaryAccountValue(quantityIn: Int, totalPriceIn: Int, quantityOut: Int, totalPriceOut: Int) extends CanMerge[StockSummaryAccountValue] {
  @inline def +(that: StockSummaryAccountValue) = StockSummaryAccountValue(this.quantityIn + that.quantityIn, this.totalPriceIn + that.totalPriceIn, this.quantityOut + that.quantityOut, this.totalPriceOut + that.totalPriceOut)

  @inline def -(that: StockSummaryAccountValue) = StockSummaryAccountValue(this.quantityIn - that.quantityIn, this.totalPriceIn - that.totalPriceIn, this.quantityOut - that.quantityOut, this.totalPriceOut - that.totalPriceOut)

  @inline def isEmpty = quantityIn == 0 && totalPriceIn == 0 && quantityOut == 0 && totalPriceOut == 0

  @inline def neg() = StockSummaryAccountValue(-quantityIn, -totalPriceIn, -quantityOut, -totalPriceOut)
}

sealed abstract class StockSummaryAccount extends DataView[StockSummaryAccountKey, StockSummaryAccountValue] {

  def dateFold(datetime: DateTime): DateTime

  extractFrom(ProductFlowDataBoard) { (o, db) =>
    val k = StockSummaryAccountKey(o.toStockID, o.productID, o.datetime)
    val v = StockSummaryAccountValue(o.quantity, o.totalValue, 0, 0)
    val k2 = StockSummaryAccountKey(o.fromStockID, o.productID, o.datetime)
    val v2 = StockSummaryAccountValue(0, 0, o.quantity, o.totalValue)
    Map(k -> v, k2 -> v2)
  }
}

object StockDailyAccount extends StockSummaryAccount {

  def dateFold(datetime: DateTime) = datetime.dayFloor
}

object StockMonthlyAccount extends StockSummaryAccount {

  def dateFold(datetime: DateTime) = datetime.monthFloor
}

object StockYearlyAccount extends StockSummaryAccount {

  def dateFold(datetime: DateTime) = datetime.yearFloor
}

@packable
case class ProductSNTraceKey(serialNumber: String, datetime: DateTime, bizRefID: Long)

@packable
case class ProductSNTraceValue(productID: Long, fromStockID: Long, toStockID: Long)

object ProductSNTrace extends DataView[ProductSNTraceKey, ProductSNTraceValue] {
  extractFrom(ProductFlowDataBoard) { (o, db) =>
    val k = ProductSNTraceKey(o.serialNumber, o.datetime, o.bizRefID)
    val v = ProductSNTraceValue(o.productID, o.fromStockID, o.toStockID)
    Map(k -> v)
  }
}

object TestService extends RemoteService {
  lazy val productIDs = new Array[Long](1000)
  lazy val providerIDs = new Array[Long](100)
  lazy val warehouseIDs = new Array[Long](5)
  lazy val customerIDs = new Array[Long](1000)

  def initCustomers(c: PersistCallContext) = {
    import c._
    val start = System.currentTimeMillis
    val pc = CustomerCollection
    var i = customerIDs.length - 1
    while (i >= 0) {
      val code = "customer-" + i
      val exist = pc.byCode(code)
      if (exist.isEmpty) {
        val p = Customer(code, code, "Customer")
        pc.insert(p)
        customerIDs(i) = p.id
      } else {
        customerIDs(i) = exist.get.id
      }
      i -= 1
    }
    val cost = System.currentTimeMillis - start
    println(s"init ${customerIDs.length} customer cost $cost ms")
  }

  def initProducts(c: PersistCallContext) = {
    import c._
    val pc = ProductCollection
    var i = productIDs.length - 1
    val start = System.currentTimeMillis
    while (i >= 0) {
      val code = "product-" + i
      val exist = pc.byCode(code)
      if (exist.isEmpty) {
        val p = Product(code, code, "Product", randomPrice(), randomPrice())
        pc.insert(p)
        productIDs(i) = p.id
      } else {
        productIDs(i) = exist.get.id
      }
      i -= 1
    }
    val cost = System.currentTimeMillis - start
    println(s"init ${productIDs.length} products cost $cost ms")
  }

  def initProviders(c: PersistCallContext) = {
    import c._
    val pc = ProviderCollection
    var i = providerIDs.length - 1
    val start = System.currentTimeMillis
    while (i >= 0) {
      val code = "provider-" + i
      val exist = pc.byCode(code)
      if (exist.isEmpty) {
        val p = Provider(code, code, "Provider")
        pc.insert(p)
        providerIDs(i) = p.id
      } else {
        providerIDs(i) = exist.get.id
      }
      i -= 1
    }
    val cost = System.currentTimeMillis - start
    println(s"init ${providerIDs.length} providers cost $cost ms")
  }

  def initWarehouses(c: PersistCallContext) = {
    import c._
    val pc = WarehouseCollection
    var i = warehouseIDs.length - 1
    val start = System.currentTimeMillis
    while (i >= 0) {
      val code = "warehouse-" + i
      val exist = pc.byCode(code)
      if (exist.isEmpty) {
        val p = Warehouse(code, code, "Warehouse")
        pc.insert(p)
        warehouseIDs(i) = p.id
      } else {
        warehouseIDs(i) = exist.get.id
      }
      i -= 1
    }
    val cost = System.currentTimeMillis - start
    println(s"init ${warehouseIDs.length} warehouse cost $cost ms")
  }

  val init: PersistCall = { c =>
    import c._
    initWarehouses(c)
    initCustomers(c)
    initProviders(c)
    initProducts(c)
    complete(model())
  }

  val r = new Random()

  def randomProvider() = RefSome(r.nextInt(providerIDs.length))

  def randomProduct() = RefSome(r.nextInt(productIDs.length))

  def randomWarehouse() = RefSome(r.nextInt(warehouseIDs.length))

  def randomSN() = r.nextLong().toString

  def randomPrice() = r.nextInt() * 1000 + 10

  def randomItems[T](f: => T): Seq[T] = {
    var i = r.nextInt(10)
    var rb = List.empty[T]
    while (i >= 0) {
      rb ::= f
      i -= 1
    }
    rb
  }


  private var datetime = new DateTime()

  def nextDt(): DateTime = {
    datetime = datetime.plusSeconds(1)
    datetime
  }

  def ensureStock(c: PersistCallContext, rentOutOrder: RentOutOrder): Unit = {
    import c._
    case class PInfo(dt: DateTime, product: Ref[Product], qty: Int, sn: String, totalValue: Int, warehouse: Ref[Warehouse])
    rentOutOrder.products.flatMap { p =>
      p.outs.map { out => (out.datetime, PurchaseOrderItem(p.product, out.serialNumber, out.quantity, out.totalValue, out.from, "by in"))}
    }.groupBy { case (dt, poi) =>
      dt
    } foreach { case (dt, x) =>
      val number = "n-" + r.nextInt()
      val ri = r.nextDouble()
      if (ri > 0.9) {
        val p = PurchaseOrder(dt, number, randomProvider(), "invoice number", "remark", x.map { case (_, poi) => poi})
        PurchaseOrderCollection.insert(p)
      } else {
        val p = RentInOrder(dt, number, randomProvider(), "remark", x.map { case (_, poi) => RentInOrderItem(poi.product, poi.serialNumber, poi.quantity, poi.totalValue, randomPrice(), poi.stock, "rent in")})
        RentInOrderCollection.insert(p)
      }
    }
  }

  def tryOpenRentOut(c: PersistCallContext, dt: DateTime): Boolean = {
    import c._
    val bookingOrder = {
      val cursor = RentOutOrderCollection.byNoneClosed((RentOutOrderState.Booking, new DateTime(0)) --+(RentOutOrderState.Booking, dt), forward = false)
      try {
        if (!cursor.isValid) {
          return false
        }
        cursor.value
      } finally {
        cursor.close()
      }
    }
    val openOrder = bookingOrder.copy(
      state = RentOutOrderState.Open,
      products = bookingOrder.products.map(
        i => i.copy(
          outs = {
            val productPrice = i.product().price
            for (x <- 1 to i.quantity) yield RentOutProduct(dt, 1, randomSN(), randomWarehouse(), productPrice, "rent out")
          }
        )
      )
    )
    ensureStock(c, openOrder)
    RentOutOrderCollection.update(openOrder)
    true
  }

  def tryCloseRentOut(c: PersistCallContext, dt: DateTime): Boolean = {
    import c._
    val openOrder = {
      val cursor = RentOutOrderCollection.byNoneClosed((RentOutOrderState.Open, new DateTime(0)) --+(RentOutOrderState.Open, dt.plusSeconds(-1000)), forward = false)
      try {
        if (!cursor.isValid) {
          return false
        }
        cursor.value
      } finally {
        cursor.close()
      }
    }
    val closeOrder = openOrder.copy(
      state = RentOutOrderState.Closed,
      products = openOrder.products.map(
        i => i.copy(
          returns = {
            i.outs map { x =>
              RentOutProductReturn(dt, x.quantity, x.serialNumber, x.from, x.totalValue, "rent return")
            }
          }
        )
      )
    )
    RentOutOrderCollection.update(openOrder)
    true
  }

  def insertOrders(c: PersistCallContext) = {
    import c._
    val dt = nextDt()
    if (!tryOpenRentOut(c, dt)) {
      tryCloseRentOut(c, dt)
    }
  }

}