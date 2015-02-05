package com.weez.mercury.product

import com.github.nscala_time.time.Imports
import com.weez.mercury.common._
import com.github.nscala_time.time.Imports._
import DTOHelper._

class WarehouseService extends MasterDataService[Warehouse] {
  override def dataCollection: MasterDataCollection[Warehouse] = WarehouseCollection
}

/**
 * 仓库
 */
@packable
case class Warehouse(code: String,
                     title: String,
                     description: String) extends MasterData

object WarehouseCollection extends MasterDataCollection[Warehouse] {
  override def name: String = "warehouse"
}

@packable
case class WAKey(warehouse: Ref[Warehouse], product: Ref[Product], datetime: DateTime, bizRefID: Long)

@packable
case class WAValue(quantity: Int, totalPrice: Int) {
  @inline final def +(that: WAValue) = WAValue(this.quantity + this.quantity, that.totalPrice + that.totalPrice)

  @inline final def -(that: WAValue) = WAValue(this.quantity - this.quantity, that.totalPrice - that.totalPrice)

  @inline final def isEmpty = quantity == 0 && totalPrice == 0

  @inline final def isDefined = !isEmpty

  @inline final def asOption = if (isEmpty) None else Some(this)
}

object StockAccount extends DataView[WAKey, WAValue] {
  override def name: String = "stock-account"
}

@packable
case class WSAKey(warehouse: Ref[Warehouse], product: Ref[Product], datetime: DateTime)

abstract class StockSummaryAccount extends DataView[WSAKey, WAValue] {
  override protected def defMerger = Some(
    new Merger[WAValue] {
      def sub(v1: WAValue, v2: WAValue) = (v1 - v2).asOption

      def add(v1: WAValue, v2: WAValue) = (v1 + v2).asOption
    }
  )

  def dateFold(datetime: DateTime): DateTime

  defExtractor(StockAccount) {
    (k, v, db) => Map(
      WSAKey(k.warehouse, k.product, dateFold(k.datetime)) -> v
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