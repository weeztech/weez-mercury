package com.weez.mercury.product

import com.weez.mercury.imports._
import com.weez.mercury.common.Range._
import com.weez.mercury.common.DTOHelper._

import scala.collection.mutable
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

case class ProductFlow(bizRef: Ref[Entity],
                       datetime: DateTime,
                       fromStock: Ref[Entity],
                       toStock: Ref[Entity],
                       product: Ref[Product],
                       quantity: Int,
                       totalValue: Int)

object ProductFlowDataBoard extends DataBoard[ProductFlow]

case class SingleProductFlow(bizRef: Ref[Entity],
                             datetime: DateTime,
                             fromStock: Ref[Entity],
                             toStock: Ref[Entity],
                             product: Ref[Product],
                             serialNumber: String,
                             totalValue: Int,
                             away: Boolean)

object SingleProductFlowDataBoard extends DataBoard[SingleProductFlow]

/**
 * 库存明细账－主键部分
 * @param stock 库，可能是仓库，或部门
 * @param product 物品
 * @param datetime 发生时间
 * @param peerStock 对应库
 * @param bizRef 业务参考，各种单据
 */
@packable
final case class StockAccountKey(stock: Ref[Entity], product: Ref[Product], datetime: DateTime, peerStock: Ref[Entity], bizRef: Ref[Entity]) {
  def revert() = StockAccountKey(peerStock, product, datetime, stock, bizRef)
}

/**
 * 物品数量金额
 * @param quantity 数量
 * @param totalValue 价值
 */
@packable
final case class ProductQV(quantity: Int,
                           totalValue: Int) extends CanMerge[ProductQV] {

  @inline def +(that: ProductQV) = ProductQV(this.quantity + that.quantity, this.totalValue + that.totalValue)

  @inline def -(that: ProductQV) = ProductQV(this.quantity - that.quantity, this.totalValue - that.totalValue)

  @inline def isEmpty = quantity == 0 && totalValue == 0

  @inline def neg() = ProductQV(-quantity, -totalValue)
}


object StockAccount extends DataView[StockAccountKey, ProductQV] with NoMergeInDB {

  extractFrom(ProductFlowDataBoard) { (o, db) =>
    val k = StockAccountKey(o.toStock, o.product, o.datetime, o.fromStock, o.bizRef)
    val v = ProductQV(o.quantity, o.totalValue)
    (k, v) ::(k.revert(), v.neg()) :: Nil
  }

  val SummaryAccounts = Seq(StockDailyAccount, StockYearlyAccount, StockMonthlyAccount)

  def inventory(stock: Ref[Entity], product: Ref[Product], dt: DateTime)(implicit db: DBSessionQueryable): ProductQV = {
    val base = new ProductSummaryQVBuffer()
    SummaryAccounts foreach { sa =>
      sa.sum(stock, product, dt, base)
    }
    import com.weez.mercury.common.Range._
    val day = dt.dayFloor
    val range = StockAccountKey(stock, product, day, RefEmpty, RefEmpty) +-+ StockAccountKey(stock, product, day.plusDays(1), RefEmpty, RefEmpty)
    val cur = this(range)
    try {
      while (cur.isValid) {
        base + cur.value.value
        cur.next()
      }
    } finally {
      cur.close()
    }
    base.toQV
  }
}

@packable
final case class StockSummaryAccountKey(stock: Ref[Entity], product: Ref[Product], datetime: DateTime)

@packable
final case class ProductSummaryQV(quantityIn: Int, totalPriceIn: Int, quantityOut: Int, totalPriceOut: Int) extends CanMerge[ProductSummaryQV] {
  def this() = this(0, 0, 0, 0)

  @inline def +(that: ProductSummaryQV) = ProductSummaryQV(this.quantityIn + that.quantityIn, this.totalPriceIn + that.totalPriceIn, this.quantityOut + that.quantityOut, this.totalPriceOut + that.totalPriceOut)

  @inline def -(that: ProductSummaryQV) = ProductSummaryQV(this.quantityIn - that.quantityIn, this.totalPriceIn - that.totalPriceIn, this.quantityOut - that.quantityOut, this.totalPriceOut - that.totalPriceOut)

  @inline def isEmpty = quantityIn == 0 && totalPriceIn == 0 && quantityOut == 0 && totalPriceOut == 0

  @inline def neg() = ProductSummaryQV(-quantityIn, -totalPriceIn, -quantityOut, -totalPriceOut)
}

trait DateFold {
  def parentDateFold(datetime: DateTime): DateTime

  def dateFold(datetime: DateTime): DateTime
}

trait DailyDateFold extends DateFold {
  final def parentDateFold(datetime: DateTime): DateTime = datetime.monthFloor

  final def dateFold(datetime: DateTime) = datetime.dayFloor
}

trait MonthlyDateFold extends DateFold {

  def parentDateFold(datetime: DateTime): DateTime = datetime.yearFloor

  def dateFold(datetime: DateTime) = datetime.monthFloor
}

trait YearlyDateFold extends DateFold {
  private val minDate = new DateTime(0)

  def parentDateFold(datetime: DateTime): DateTime = minDate

  def dateFold(datetime: DateTime) = datetime.yearFloor
}

class ProductSummaryQVBuffer {
  var quantityIn: Int = 0
  var totalPriceIn: Int = 0
  var quantityOut: Int = 0
  var totalPriceOut: Int = 0

  def +(qv: ProductSummaryQV): Unit = {
    quantityIn += qv.quantityIn
    totalPriceIn += qv.totalPriceIn
    quantityOut += qv.quantityOut
    totalPriceOut += qv.totalPriceOut
  }

  def -(qv: ProductSummaryQV): Unit = {
    quantityIn -= qv.quantityIn
    totalPriceIn -= qv.totalPriceIn
    quantityOut -= qv.quantityOut
    totalPriceOut -= qv.totalPriceOut
  }

  def +(qv: ProductQV): Unit = {
    if (qv.quantity > 0) {
      quantityIn += qv.quantity
      totalPriceIn += qv.totalValue
    } else {
      quantityOut -= qv.quantity
      totalPriceOut -= qv.totalValue
    }
  }

  def -(qv: ProductQV): Unit = {
    if (qv.quantity < 0) {
      quantityIn += qv.quantity
      totalPriceIn += qv.totalValue
    } else {
      quantityOut -= qv.quantity
      totalPriceOut -= qv.totalValue
    }
  }

  def toQV = ProductQV(quantityIn - quantityOut, totalPriceIn - totalPriceOut)

  def toSummaryQV = ProductSummaryQV(quantityIn, totalPriceIn, quantityOut, totalPriceOut)
}

final class MappedPSQVBuffer {
  private val map = mutable.LongMap.empty[ProductSummaryQVBuffer]

  def add(key: Ref[Entity], qv: ProductSummaryQV): Unit = {
    map.getOrElseUpdate(key.id, new ProductSummaryQVBuffer()) + qv
  }

  def add(key: Ref[Entity], qv: ProductQV): Unit = {
    map.getOrElseUpdate(key.id, new ProductSummaryQVBuffer()) + qv
  }

  def sub(key: Ref[Entity], qv: ProductQV): Unit = {
    map.getOrElseUpdate(key.id, new ProductSummaryQVBuffer()) - qv
  }

  def toMap = {
    map.map { case (k, v) => (RefSome[Entity](k), v.toQV)}.toMap[Ref[Entity], ProductQV]
  }
}

object ProductSummaryQVBuffer {
  def apply = new ProductSummaryQVBuffer

  def reduce(key: Long, map: mutable.LongMap[ProductSummaryQVBuffer])(f: ProductSummaryQVBuffer => Unit): Unit = {
    f(map.getOrElseUpdate(key, new ProductSummaryQVBuffer))
  }
}

sealed abstract class StockSummaryAccount extends DataView[StockSummaryAccountKey, ProductSummaryQV] with DateFold {

  extractFrom(ProductFlowDataBoard) { (o, db) =>
    val dt = dateFold(o.datetime)
    val k = StockSummaryAccountKey(o.toStock, o.product, dt)
    val v = ProductSummaryQV(o.quantity, o.totalValue, 0, 0)
    val k2 = StockSummaryAccountKey(o.fromStock, o.product, dt)
    val v2 = ProductSummaryQV(0, 0, o.quantity, o.totalValue)
    (k, v) ::(k2, v2) :: Nil
  }

  def sum(stock: Ref[Entity], product: Ref[Product], dt: DateTime, buf: ProductSummaryQVBuffer)(implicit db: DBSessionQueryable): Unit = {
    import com.weez.mercury.common.Range._
    val range = StockSummaryAccountKey(stock, product, parentDateFold(dt)) +-- StockSummaryAccountKey(stock, product, dateFold(dt))
    for (kv <- this(range)) {
      buf + kv.value
    }
  }
}


object StockDailyAccount extends StockSummaryAccount with DailyDateFold

object StockMonthlyAccount extends StockSummaryAccount with MonthlyDateFold

object StockYearlyAccount extends StockSummaryAccount with YearlyDateFold

@packable
final case class ProductAccountKey(product: Ref[Product], datetime: DateTime,
                                   fromStock: Ref[Entity], toStock: Ref[Entity], bizRef: Ref[Entity])

object ProductAccount extends DataView[ProductAccountKey, ProductQV] with NoMergeInDB {

  extractFrom(ProductFlowDataBoard) { (o, db) =>
    val k = ProductAccountKey(o.product, o.datetime, o.fromStock, o.toStock, o.bizRef)
    val v = ProductQV(o.quantity, o.totalValue)
    (k, v) :: Nil
  }

  val SummaryAccounts = Seq(ProductDailyAccount, ProductYearlyAccount, ProductMonthlyAccount)

  def inventory(product: Ref[Product], dt: DateTime)(implicit db: DBSessionQueryable): ProductQV = {
    val buf = new ProductSummaryQVBuffer
    for (sa <- SummaryAccounts) {
      sa.sum(product, dt, buf)
    }
    import com.weez.mercury.common.Range._
    val day = dt.dayFloor
    val range = ProductAccountKey(product, day, RefEmpty, RefEmpty, RefEmpty) --- ProductAccountKey(product, day.plusDays(1), RefEmpty, RefEmpty, RefEmpty)
    for (kv <- this(range)) {
      buf + kv.value
    }
    buf.toQV
  }

  def inventoryByStock(product: Ref[Product], dt: DateTime)(implicit db: DBSessionQueryable): Map[Ref[Entity], ProductQV] = {
    val buf = new MappedPSQVBuffer
    for (sa <- SummaryAccounts) {
      sa.sum(product, dt, buf)
    }
    import com.weez.mercury.common.Range._
    val day = dt.dayFloor
    val range = ProductAccountKey(product, day, RefEmpty, RefEmpty, RefEmpty) --- ProductAccountKey(product, day.plusDays(1), RefEmpty, RefEmpty, RefEmpty)
    for (kv <- this(range)) {
      buf.add(kv.key.toStock, kv.value)
      buf.sub(kv.key.fromStock, kv.value)
    }
    buf.toMap
  }
}

@packable
final case class ProductSummaryAccountKey(product: Ref[Product], datetime: DateTime, stock: Ref[Entity])

sealed abstract class ProductSummaryAccount extends DataView[ProductSummaryAccountKey, ProductSummaryQV] with DateFold {

  extractFrom(ProductFlowDataBoard) { (o, db) =>
    val dt = dateFold(o.datetime)
    val k = ProductSummaryAccountKey(o.product, dt, o.toStock)
    val v = ProductSummaryQV(o.quantity, o.totalValue, 0, 0)
    val k2 = ProductSummaryAccountKey(o.product, dt, o.fromStock)
    val v2 = ProductSummaryQV(0, 0, o.quantity, o.totalValue)
    (k, v) ::(k2, v2) :: Nil
  }

  def sum(product: Ref[Product], dt: DateTime, buf: ProductSummaryQVBuffer)(implicit db: DBSessionQueryable): Unit = {
    import com.weez.mercury.common.Range._
    val range = ProductSummaryAccountKey(product, parentDateFold(dt), RefEmpty) --- ProductSummaryAccountKey(product, dateFold(dt), RefEmpty)
    for (kv <- this(range)) {
      buf + kv.value
    }
  }

  def sum(product: Ref[Product], dt: DateTime, buf: MappedPSQVBuffer)(implicit db: DBSessionQueryable) = {
    import com.weez.mercury.common.Range._
    val range = ProductSummaryAccountKey(product, parentDateFold(dt), RefEmpty) --- ProductSummaryAccountKey(product, dateFold(dt), RefEmpty)
    for (kv <- this(range)) {
      buf.add(kv.key.stock, kv.value)
    }
  }
}

object ProductDailyAccount extends ProductSummaryAccount with DailyDateFold

object ProductMonthlyAccount extends ProductSummaryAccount with MonthlyDateFold

object ProductYearlyAccount extends ProductSummaryAccount with YearlyDateFold


@packable
final case class SingleProductAccountKey(serialNumber: String, datetime: DateTime, bizRef: Ref[Entity])

@packable
final case class SingleProductAccountValue(product: Ref[Product], fromStock: Ref[Entity], toStock: Ref[Entity])

object SingleProductAccount extends DataView[SingleProductAccountKey, SingleProductAccountValue] {
  extractFrom(SingleProductFlowDataBoard) { (o, db) =>
    val k = SingleProductAccountKey(o.serialNumber, o.datetime, o.bizRef)
    val v = SingleProductAccountValue(o.product, o.fromStock, o.toStock)
    (k, v) :: Nil
  }
}

@packable
final case class SingleProductByStockInventoryKey(stock: Ref[Entity], product: Ref[Product], datetime: DateTime, serialNumber: String)

@packable
final case class SingleProductByStockInventoryValue(fromStock: Ref[Entity], bizRef: Ref[Entity])

object SingleProductInventoryByStock extends DataView[SingleProductByStockInventoryKey, SingleProductByStockInventoryValue] {
  extractFrom(SingleProductFlowDataBoard) { (o, db) =>
    if (o.away) {
      List.empty
    } else {
      val k = SingleProductByStockInventoryKey(o.toStock, o.product, o.datetime, o.serialNumber)
      val v = SingleProductByStockInventoryValue(o.fromStock, o.bizRef)
      (k, v) :: Nil
    }
  }
}

@packable
final case class MaxDateTime(dt: DateTime) extends CanMerge[MaxDateTime] {
  def isEmpty: Boolean = dt eq null

  def neg(): MaxDateTime = MaxDateTime(null)

  def +(x: MaxDateTime): MaxDateTime = {
    if (isEmpty || dt.getMillis < x.dt.getMillis) {
      x
    } else {
      this
    }
  }

  def -(x: MaxDateTime): MaxDateTime = this
}

object MaxProductFlowDate extends DataView[Boolean, MaxDateTime] {
  extractFrom(ProductFlowDataBoard) { (o, db) =>
    (true, MaxDateTime(o.datetime)) :: Nil
  }
}
