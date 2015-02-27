package com.weez.mercury.product

import com.weez.mercury.imports._
import com.weez.mercury.common.DTOHelper._
import com.github.nscala_time.time.Imports._
import com.weez.mercury.product.RentInOrderCollection._

@packable
case class PurchaseOrderItem(stock: Ref[Warehouse],
                             product: Ref[Product],
                             quantity: Int,
                             totalValue: Int,
                             singleProducts: Seq[SingleProductInfo],
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
      ProductFlow(o.id, o.datetime, o.provider.id, item.stock.id, item.product.id, item.quantity, item.totalValue)
    }
  }
  extractTo(SingleProductFlowDataBoard) { (o, db) =>
    o.items.flatMap { item =>
      val tv = item.totalValue / item.quantity
      item.singleProducts.map { s =>
        SingleProductFlow(o.id, o.datetime, o.provider.id, item.stock.id, item.product.id, s.serialNumber, tv, s.nextBizRefID != 0l)
      }
    }
  }
}

@packable
case class PurchaseReturnItem(stock: Ref[Warehouse],
                              product: Ref[Product],
                              quantity: Int,
                              totalValue: Int,
                              singleProducts: Seq[SingleProductInfo],
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
      ProductFlow(o.id, o.datetime, item.stock.id, o.provider.id, item.product.id, item.quantity, item.totalValue)
    }
  }
  extractTo(SingleProductFlowDataBoard) { (o, db) =>
    o.items.flatMap { item =>
      val tv = item.totalValue / item.quantity
      item.singleProducts.map { s =>
        SingleProductFlow(o.id, o.datetime, o.provider.id, item.stock.id, item.product.id, s.serialNumber, tv, s.nextBizRefID != 0l)
      }
    }
  }

}