package com.weez.mercury.product

import com.github.nscala_time.time.Imports._
import com.weez.mercury.imports._
import com.weez.mercury.common.DTOHelper._

/**
 * 仓库
 */
@packable
final case class Warehouse(code: String,
                           title: String,
                           description: String) extends MasterData

object WarehouseCollection extends RootCollection[Warehouse] {
  override def name: String = "warehouse"
}

@packable
case class Provider(code: String,
                    title: String, description: String) extends MasterData

object ProviderCollection extends RootCollection[Provider] {
  override def name: String = "provider"
}

/**
 * 库存明细账－主键部分
 * @param stockID 库ID，可能是仓库，或部门
 * @param productID 物品ID
 * @param datetime 发生时间
 * @param bizRefID 业务参考ID，各种单据的ID
 */
@packable
final case class StockAccountKey(stockID: Long, productID: Long, datetime: DateTime, bizRefID: Long)

/**
 * 库存明晰帐－值部分
 * @param quantity 数量
 * @param totalPrice 价值
 */
@packable
final case class StockAccountValue(quantity: Int,
                                   totalPrice: Int) {
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
   * 租入
   */
  defExtractor(RentInOrderCollection) { (r, db) =>
    r.items.map { item =>
      StockAccountKey(
        stockID = item.warehouse.id,
        productID = item.product.id,
        datetime = r.datetime,
        bizRefID = r.id
      ) -> StockAccountValue(
        quantity = item.quantity,
        totalPrice = item.productsValue
      )
    }.toMap
  }

  /**
   * 采购
   */
  defExtractor(PurchaseOrderCollection) { (r, db) =>
    r.items.map { item =>
      StockAccountKey(
        stockID = item.warehouse.id,
        productID = item.product.id,
        datetime = r.datetime,
        bizRefID = r.id
      ) -> StockAccountValue(
        quantity = item.quantity,
        totalPrice = item.productsValue
      )
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
