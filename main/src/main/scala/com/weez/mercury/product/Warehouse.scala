package com.weez.mercury.product

import com.weez.mercury.common.{NotMergeInDB, RefSome}
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

case class ProductFlow(bizRefID: Long,
                       datetime: DateTime,
                       fromStockID: Long,
                       toStockID: Long,
                       productID: Long,
                       quantity: Int,
                       totalValue: Int)

object ProductFlowDataBoard extends DataBoard[ProductFlow]

case class SingleProductFlow(bizRefID: Long,
                             datetime: DateTime,
                             fromStockID: Long,
                             toStockID: Long,
                             productID: Long,
                             serialNumber: String,
                             totalValue: Int,
                             away: Boolean)

object SingleProductFlowDataBoard extends DataBoard[SingleProductFlow]

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


object StockAccount extends DataView[StockAccountKey, ProductQV] with NotMergeInDB {

  extractFrom(ProductFlowDataBoard) { (o, db) =>
    val k = StockAccountKey(o.toStockID, o.productID, o.datetime, o.fromStockID, o.bizRefID)
    val v = ProductQV(o.quantity, o.totalValue)
    (k, v) ::(k.revert(), v.neg()) :: Nil
  }

  val SummaryAccounts = Seq(StockDailyAccount, StockYearlyAccount, StockMonthlyAccount)

  def inventory(stockID: Long, productID: Long, dt: DateTime)(implicit db: DBSessionQueryable): ProductQV = {
    val base = new ProductSummaryQVBuffer()
    SummaryAccounts foreach { sa =>
      sa.sum(stockID, productID, dt, base)
    }
    import com.weez.mercury.common.Range._
    val day = dt.dayFloor
    val range = StockAccountKey(stockID, productID, day, 0, 0) +-+ StockAccountKey(stockID, productID, day.plusDays(1), 0, 0)
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
final case class StockSummaryAccountKey(stockID: Long, productID: Long, datetime: DateTime)

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

  def add(key: Long, qv: ProductSummaryQV): Unit = {
    map.getOrElseUpdate(key, new ProductSummaryQVBuffer()) + qv
  }

  def add(key: Long, qv: ProductQV): Unit = {
    map.getOrElseUpdate(key, new ProductSummaryQVBuffer()) + qv
  }

  def sub(key: Long, qv: ProductQV): Unit = {
    map.getOrElseUpdate(key, new ProductSummaryQVBuffer()) - qv
  }

  def toMap = {
    map.mapValues(_.toQV).toMap[Long, ProductQV]
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
    val k = StockSummaryAccountKey(o.toStockID, o.productID, dt)
    val v = ProductSummaryQV(o.quantity, o.totalValue, 0, 0)
    val k2 = StockSummaryAccountKey(o.fromStockID, o.productID, dt)
    val v2 = ProductSummaryQV(0, 0, o.quantity, o.totalValue)
    (k, v) ::(k2, v2) :: Nil
  }

  def sum(stockID: Long, productID: Long, dt: DateTime, buf: ProductSummaryQVBuffer)(implicit db: DBSessionQueryable): Unit = {
    import com.weez.mercury.common.Range._
    val range = StockSummaryAccountKey(stockID, productID, parentDateFold(dt)) +-- StockSummaryAccountKey(stockID, productID, dateFold(dt))
    for (kv <- this(range)) {
      buf + kv.value
    }
  }
}


object StockDailyAccount extends StockSummaryAccount with DailyDateFold

object StockMonthlyAccount extends StockSummaryAccount with MonthlyDateFold

object StockYearlyAccount extends StockSummaryAccount with YearlyDateFold

@packable
final case class ProductAccountKey(productID: Long, datetime: DateTime, fromStockID: Long, toStockID: Long, bizRefID: Long)

object ProductAccount extends DataView[ProductAccountKey, ProductQV] with NotMergeInDB {

  extractFrom(ProductFlowDataBoard) { (o, db) =>
    val k = ProductAccountKey(o.productID, o.datetime, o.fromStockID, o.toStockID, o.bizRefID)
    val v = ProductQV(o.quantity, o.totalValue)
    (k, v) :: Nil
  }

  val SummaryAccounts = Seq(ProductDailyAccount, ProductYearlyAccount, ProductMonthlyAccount)

  def inventory(productID: Long, dt: DateTime)(implicit db: DBSessionQueryable): ProductQV = {
    val buf = new ProductSummaryQVBuffer
    for (sa <- SummaryAccounts) {
      sa.sum(productID, dt, buf)
    }
    import com.weez.mercury.common.Range._
    val day = dt.dayFloor
    val range = ProductAccountKey(productID, day, 0, 0, 0) +-+ ProductAccountKey(productID, day.plusDays(1), 0, 0, 0)
    for (kv <- this(range)) {
      buf + kv.value
    }
    buf.toQV
  }

  def inventoryByStock(productID: Long, dt: DateTime)(implicit db: DBSessionQueryable): Map[Long, ProductQV] = {
    val buf = new MappedPSQVBuffer
    for (sa <- SummaryAccounts) {
      sa.sum(productID, dt, buf)
    }
    import com.weez.mercury.common.Range._
    val day = dt.dayFloor
    val range = ProductAccountKey(productID, day, 0, 0, 0) +-+ ProductAccountKey(productID, day.plusDays(1), 0, 0, 0)
    for (kv <- this(range)) {
      buf.add(kv.key.toStockID, kv.value)
      buf.sub(kv.key.fromStockID, kv.value)
    }
    buf.toMap
  }
}

@packable
final case class ProductSummaryAccountKey(productID: Long, datetime: DateTime, stockID: Long)

sealed abstract class ProductSummaryAccount extends DataView[ProductSummaryAccountKey, ProductSummaryQV] with DateFold {

  extractFrom(ProductFlowDataBoard) { (o, db) =>
    val dt = dateFold(o.datetime)
    val k = ProductSummaryAccountKey(o.productID, dt, o.toStockID)
    val v = ProductSummaryQV(o.quantity, o.totalValue, 0, 0)
    val k2 = ProductSummaryAccountKey(o.productID, dt, o.fromStockID)
    val v2 = ProductSummaryQV(0, 0, o.quantity, o.totalValue)
    (k, v) ::(k2, v2) :: Nil
  }

  def sum(productID: Long, dt: DateTime, buf: ProductSummaryQVBuffer)(implicit db: DBSessionQueryable): Unit = {
    import com.weez.mercury.common.Range._
    val range = ProductSummaryAccountKey(productID, parentDateFold(dt), 0l) +-- ProductSummaryAccountKey(productID, dateFold(dt), 0)
    for (kv <- this(range)) {
      buf + kv.value
    }
  }

  def sum(productID: Long, dt: DateTime, buf: MappedPSQVBuffer)(implicit db: DBSessionQueryable) = {
    import com.weez.mercury.common.Range._
    val range = ProductSummaryAccountKey(productID, parentDateFold(dt), 0l) +-- ProductSummaryAccountKey(productID, dateFold(dt), 0)
    for (kv <- this(range)) {
      buf.add(kv.key.stockID, kv.value)
    }
  }
}

object ProductDailyAccount extends ProductSummaryAccount with DailyDateFold

object ProductMonthlyAccount extends ProductSummaryAccount with MonthlyDateFold

object ProductYearlyAccount extends ProductSummaryAccount with YearlyDateFold


@packable
final case class SingleProductAccountKey(serialNumber: String, datetime: DateTime, bizRefID: Long)

@packable
final case class SingleProductAccountValue(productID: Long, fromStockID: Long, toStockID: Long)

object SingleProductAccount extends DataView[SingleProductAccountKey, SingleProductAccountValue] {
  extractFrom(SingleProductFlowDataBoard) { (o, db) =>
    val k = SingleProductAccountKey(o.serialNumber, o.datetime, o.bizRefID)
    val v = SingleProductAccountValue(o.productID, o.fromStockID, o.toStockID)
    (k, v) :: Nil
  }
}

@packable
final case class SingleProductByStockInventoryKey(stockID: Long, productID: Long, datetime: DateTime, serialNumber: String)

@packable
final case class SingleProductByStockInventoryValue(fromStockID: Long, bizRefID: Long)

object SingleProductInventoryByStock extends DataView[SingleProductByStockInventoryKey, SingleProductByStockInventoryValue] {
  extractFrom(SingleProductFlowDataBoard) { (o, db) =>
    if (o.away) {
      List.empty
    } else {
      val k = SingleProductByStockInventoryKey(o.toStockID, o.productID, o.datetime, o.serialNumber)
      val v = SingleProductByStockInventoryValue(o.fromStockID, o.bizRefID)
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
