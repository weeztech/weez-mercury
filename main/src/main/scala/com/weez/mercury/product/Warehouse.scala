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
                           description: String) extends Entity

object WarehouseCollection extends RootCollection[Warehouse] {
  override def name: String = "warehouse"
}

@packable
case class Provider(code: String,
                    title: String, description: String) extends Entity

object ProviderCollection extends RootCollection[Provider] {
  override def name: String = "provider"
}

trait ProductFlowEntry {
  def product: Ref[Product]

  def serialNumber: String

  def stock: Ref[Entity]

  def quantity: Int

  def totalValue: Int
}


sealed trait ProductFlowPeer {
  def stock: Ref[Entity]
}

final case class ProductFlowFromPeer(stock: Ref[Entity]) extends ProductFlowPeer

final case class ProductFlowToPeer(stock: Ref[Entity]) extends ProductFlowPeer

trait ProductFlow extends Entity {

  def peerStock: ProductFlowPeer

  def datetime: DateTime

  def items: Seq[ProductFlowEntry]
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
 * @param totalValue 价值
 */
@packable
final case class StockAccountValue(quantity: Int,
                                   totalValue: Int) {

  @inline def +(that: StockAccountValue) = StockAccountValue(this.quantity + that.quantity, this.totalValue + that.totalValue)

  @inline def +(that: ProductFlowEntry) = StockAccountValue(this.quantity + that.quantity, this.totalValue + that.totalValue)

  @inline def -(that: StockAccountValue) = StockAccountValue(this.quantity - that.quantity, this.totalValue - that.totalValue)

  @inline def -(that: ProductFlowEntry) = StockAccountValue(this.quantity - that.quantity, this.totalValue - that.totalValue)

  @inline def isEmpty = quantity == 0 && totalValue == 0

  @inline def isDefined = !isEmpty

  @inline def asOption = if (isEmpty) None else Some(this)

  @inline def asStockSummaryAccountValue() = {
    if (quantity > 0)
      StockSummaryAccountValue(quantity, totalValue, 0, 0)
    else
      StockSummaryAccountValue(0, 0, quantity, totalValue)
  }
}


object StockAccount extends DataView[StockAccountKey, StockAccountValue] {
  override def name: String = "stock-account"

  private def defExtractors(): Unit = {
    def extractor(flow: ProductFlow, db: DBSessionQueryable) = {
      val peer = flow.peerStock
      var result = Map.empty[StockAccountKey, StockAccountValue]
      for (x <- flow.items) {
        val k = StockAccountKey(
          stockID = x.stock.id,
          productID = x.product.id,
          datetime = flow.datetime,
          bizRefID = flow.id
        )
        val exist = result.get(k)
        val v = if (exist.isEmpty) {
          peer match {
            case _: ProductFlowFromPeer =>
              StockAccountValue(x.quantity, x.totalValue)
            case _: ProductFlowToPeer =>
              StockAccountValue(-x.quantity, -x.totalValue)
          }
        } else {
          val ev = exist.get
          peer match {
            case _: ProductFlowFromPeer =>
              ev + x
            case _: ProductFlowToPeer =>
              ev - x
          }
        }
        result = result.updated(k, v)
      }
      result
    }
    val sources = Seq(
      RentInOrderCollection, //租入
      RentInReturnCollection, //租入归还
      PurchaseOrderCollection, //采购
      PurchaseReturnCollection //采购退货
    )
    for (s <- sources) {
      defExtractor(s)(extractor)
    }
  }

  defExtractors()
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

@packable
case class ProductSNTraceKey(serialNumber: String, datetime: DateTime, bizRefID: Long)

@packable
case class ProductSNTraceValue(productID: Long, fromStockID: Long, toStockID: Long)

object ProductSNTrace extends DataView[ProductSNTraceKey, ProductSNTraceValue] {
  override def name: String = "product-sn-trace"

  private def defExtractors(): Unit = {
    def extractor(flow: ProductFlow, db: DBSessionQueryable) = {
      val peer = flow.peerStock
      var result = Map.empty[ProductSNTraceKey, ProductSNTraceValue]
      for (x <- flow.items) {
        val k = ProductSNTraceKey(
          serialNumber = x.serialNumber,
          datetime = flow.datetime,
          bizRefID = flow.id
        )
        if (!result.contains(k)) {
          val v = peer match {
            case p: ProductFlowFromPeer => ProductSNTraceValue(x.product.id, p.stock.id, x.stock.id)
            case p: ProductFlowToPeer => ProductSNTraceValue(x.product.id, x.stock.id, p.stock.id)
          }
          result = result.updated(k, v)
        }
      }
      result
    }
    val sources = Seq(
      RentInOrderCollection, //租入
      RentInReturnCollection, //租入归还
      PurchaseOrderCollection, //采购
      PurchaseReturnCollection //采购退货
    )
    for (s <- sources) {
      defExtractor(s)(extractor)
    }
  }

  defExtractors()

}