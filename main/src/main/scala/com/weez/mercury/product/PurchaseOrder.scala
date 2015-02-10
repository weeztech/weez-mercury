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
                             remark: String) extends ProductFlowEntry

@packable
case class PurchaseOrder(datetime: DateTime,
                         provider: Ref[Provider],
                         number: String,
                         invoiceNumber: String,
                         remark: String,
                         items: Seq[PurchaseOrderItem]) extends Entity with ProductFlow {

  def peerStock: ProductFlowPeer = ProductFlowFromPeer(provider)
}

object PurchaseOrderCollection extends RootCollection[PurchaseOrder] {
  override def name: String = "purchase-order"
}


@packable
case class PurchaseReturnItem(product: Ref[Product],
                              serialNumber: String,
                              quantity: Int,
                              totalValue: Int,
                              stock: Ref[Warehouse],
                              remark: String) extends ProductFlowEntry

@packable
case class PurchaseReturn(datetime: DateTime,
                          provider: Ref[Provider],
                          number: String,
                          remark: String,
                          items: Seq[PurchaseReturnItem]) extends Entity with ProductFlow {
  def peerStock: ProductFlowPeer = ProductFlowToPeer(provider)
}

object PurchaseReturnCollection extends RootCollection[PurchaseReturn] {
  override def name: String = "purchase-return"
}