package com.weez.mercury.product

import com.github.nscala_time.time.Imports._
import com.weez.mercury.imports._
import com.weez.mercury.common.DTOHelper._

final class WarehouseService extends MasterDataService[Warehouse] {
  override def dataCollection: MasterDataCollection[Warehouse] = WarehouseCollection
}

/**
 * 仓库
 */
@packable
final case class Warehouse(code: String,
                           title: String,
                           description: String) extends MasterData

object WarehouseCollection extends MasterDataCollection[Warehouse] {
  override def name: String = "warehouse"
}

@packable
final case class StockAccountKey(stockID: Long, productID: Long, datetime: DateTime, bizID: Long)

@packable
final case class StockAccountValue(quantity: Int, totalPrice: Int) {
  @inline def +(that: StockAccountValue) = StockAccountValue(this.quantity + that.quantity, this.totalPrice + that.totalPrice)

  @inline def -(that: StockAccountValue) = StockAccountValue(this.quantity - that.quantity, this.totalPrice - that.totalPrice)

  @inline def isEmpty = quantity == 0 && totalPrice == 0

  @inline def isDefined = !isEmpty

  @inline def asOption = if (isEmpty) None else Some(this)

  @inline def asStockSummaryAccountValue() = {
    if (quantity > 0)
      StockSummaryAccountValue(quantity, totalPrice, 0, 0)
    else
      StockSummaryAccountValue(0, 0, quantity, totalPrice)
  }
}

object StockAccount extends DataView[StockAccountKey, StockAccountValue] {
  override def name: String = "stock-account"
  /**
   * 定义提取到库存流水表
   */
  defExtractor(RentInOrderCollection) { (r, db) =>
    r.items.map { item =>
      StockAccountKey(r.warehouse.id, item.product.id, r.datetime, r.id) -> StockAccountValue(item.quantity, item.price * item.quantity)
    }.toMap
  }
}

@packable
final case class StockSummaryAccountKey(stockID: Long, productID: Long, datetime: DateTime)

@packable
final case class StockSummaryAccountValue(quantityIn: Int, totalPriceIn: Int, quantityOut: Int, totalPriceOut: Int) {
  @inline def +(that: StockSummaryAccountValue) = StockSummaryAccountValue(this.quantityIn + that.quantityIn, this.totalPriceIn + that.totalPriceIn, this.quantityOut + that.quantityOut, this.totalPriceOut + that.totalPriceOut)

  @inline def -(that: StockSummaryAccountValue) = StockSummaryAccountValue(this.quantityIn - that.quantityIn, this.totalPriceIn - that.totalPriceIn, this.quantityOut - that.quantityOut, this.totalPriceOut - that.totalPriceOut)

  @inline def isEmpty = quantityIn == 0 && totalPriceIn == 0 && quantityOut == 0 && totalPriceOut == 0

  @inline def isDefined = !isEmpty

  @inline def asOption = if (isEmpty) None else Some(this)
}

sealed abstract class StockSummaryAccount extends DataView[StockSummaryAccountKey, StockSummaryAccountValue] {
  override protected def defMerger = Some(
    new Merger[StockSummaryAccountValue] {
      def sub(v1: StockSummaryAccountValue, v2: StockSummaryAccountValue) = (v1 - v2).asOption

      def add(v1: StockSummaryAccountValue, v2: StockSummaryAccountValue) = (v1 + v2).asOption
    }
  )

  def dateFold(datetime: DateTime): DateTime

  defExtractor(StockAccount) {
    (k, v, db) => Map(
      StockSummaryAccountKey(k.stockID, k.productID, dateFold(k.datetime)) -> v.asStockSummaryAccountValue()
    )
  }
}

object StockDailyAccount extends StockSummaryAccount {
  def name: String = "stock-account-daily"

  def dateFold(datetime: DateTime) = datetime.dayFloor
}

object StockMonthlyAccount extends StockSummaryAccount {
  def name: String = "stock-account-monthly"

  def dateFold(datetime: DateTime) = datetime.monthFloor
}

object StockYearlyAccount extends StockSummaryAccount {
  def name: String = "stock-account-yearly"

  def dateFold(datetime: DateTime) = datetime.yearFloor
}
