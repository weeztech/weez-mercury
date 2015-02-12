package com.weez.mercury.product

import com.weez.mercury.imports._
import com.weez.mercury.common.DTOHelper._
import com.github.nscala_time.time.Imports._

@packable
case class PurchaseOrderItem(product: Ref[Product],
                             serialNumber: String,
                             quantity: Int,
                             totalValue: Int,
                             stock: Ref[Warehouse],
                             remark: String)

@packable
case class PurchaseOrder(datetime: DateTime,
                         number: String,
                         provider: Ref[Provider],
                         invoiceNumber: String,
                         remark: String,
                         items: Seq[PurchaseOrderItem]) extends Entity with ProductFlowBiz {
  def productFlows() = {
    val self = this
    items map { item =>
      new ProductFlow {
        def bizRef = self

        def serialNumber: String = item.serialNumber

        def toStock = item.stock

        def datetime = self.datetime

        def product = item.product

        def fromStock = self.provider

        def totalValue = item.totalValue

        def quantity = item.quantity
      }
    }
  }
}

object PurchaseOrderCollection extends RootCollection[PurchaseOrder]


@packable
case class PurchaseReturnItem(product: Ref[Product],
                              serialNumber: String,
                              quantity: Int,
                              totalValue: Int,
                              stock: Ref[Warehouse],
                              remark: String)

@packable
case class PurchaseReturn(datetime: DateTime,
                          provider: Ref[Provider],
                          number: String,
                          remark: String,
                          items: Seq[PurchaseReturnItem]) extends Entity with ProductFlowBiz {
  def productFlows() = {
    val self = this
    items map { item =>
      new ProductFlow {
        def bizRef = self

        def serialNumber = item.serialNumber


        def toStock = self.provider

        def datetime = self.datetime

        def product = item.product

        def fromStock = item.stock

        def totalValue = item.totalValue

        def quantity = item.quantity
      }
    }
  }
}

object PurchaseReturnCollection extends RootCollection[PurchaseReturn]