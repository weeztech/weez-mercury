package com.weez.mercury.product

import com.weez.mercury.imports._
import com.weez.mercury.common.DTOHelper._
import com.github.nscala_time.time.Imports._
import com.weez.mercury.product.RentInOrderCollection._

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
                         items: Seq[PurchaseOrderItem]) extends Entity

object PurchaseOrderCollection extends RootCollection[PurchaseOrder] {
  extractTo(ProductFlowDataBoard) { (o, db) =>
    o.items.map { item =>
      ProductFlow(o.id, o.datetime, o.provider.id, item.stock.id, item.product.id, item.serialNumber, item.quantity, item.totalValue)
    }
  }
}

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
                          items: Seq[PurchaseReturnItem]) extends Entity

object PurchaseReturnCollection extends RootCollection[PurchaseReturn] {
  extractTo(ProductFlowDataBoard) { (o, db) =>
    o.items.map { item =>
      ProductFlow(o.id, o.datetime, item.stock.id, o.provider.id, item.product.id, item.serialNumber, item.quantity, item.totalValue)
    }
  }

}