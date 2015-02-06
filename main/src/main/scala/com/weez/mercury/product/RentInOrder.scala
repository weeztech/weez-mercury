package com.weez.mercury.product

import com.weez.mercury.imports._
import com.github.nscala_time.time.Imports._
import com.weez.mercury.common.DTOHelper._

@packable
case class RentInOrderSingleProduct(serialNumber: String, remark: String)

/**
 * 租入单子项
 * @param product 商品
 * @param quantity 数量
 * @param warehouse 存入仓库
 * @param productsValue 租入物品总价值
 * @param rentPrice 租金价格/每件日
 * @param singleProducts 单品信息
 */
@packable
case class RentInOrderItem(product: Ref[Product],
                           warehouse: Ref[Warehouse],
                           quantity: Int,
                           productsValue: Int,
                           rentPrice: Int,
                           remark: String,
                           singleProducts: Seq[RentInOrderSingleProduct])

/**
 * 租入单
 */
@packable
case class RentInOrder(datetime: DateTime,
                       code: String,
                       provider: Ref[Provider],
                       remark: String,
                       items: Seq[RentInOrderItem]) extends Entity

object RentInOrder {

  import MasterDataHelper._

  def toMO(rentInOrder: Option[RentInOrder])(implicit db: DBSessionQueryable): ModelObject = {
    rentInOrder.asMO { (mo, o) =>
      mo.id = o.id.toString
      mo.datetime = o.datetime
      mo.code = o.code
      mo.remark = o.remark
      mo.items = o.items.asMO { (mo, o) =>
        mo.product = o.product.codeTitleAsMO
        mo.warehouse = o.warehouse.codeTitleAsMO
        mo.quantity = o.quantity
        mo.productsValue = o.productsValue
        mo.rentPrice = o.rentPrice
        mo.remark = o.remark
        mo.serialNumbers = o.singleProducts.asMO { (mo, o) =>
          mo.serialNumber = o.serialNumber
          mo.remark = o.remark
        }
      }
    }
  }

  def apply(mo: ModelObject)(implicit db: DBSessionQueryable): Option[RentInOrder] = {
    if (mo eq null) {
      return None
    }
    val rio = RentInOrder(
      datetime = mo.dateTime,
      code = mo.code,
      provider = mo.refs.provider,
      remark = mo.remark,
      items = mo.seqs.items.map { mo =>
        RentInOrderItem(
          product = mo.refs.product,
          warehouse = mo.refs.warehouse,
          quantity = mo.quantity,
          rentPrice = mo.rentPrice,
          productsValue = mo.productsValue,
          remark = mo.remark,
          singleProducts = mo.seqs.singleProducts.map { mo =>
            RentInOrderSingleProduct(
              serialNumber = mo.serialNumber,
              remark = mo.remark
            )
          }
        )
      }
    )
    rio.id = mo.id
    Some(rio)
  }

  def get(c: RemoteService#QueryCallContext): Unit = {
    import c._
    complete(toMO(RentInOrderCollection(request.id: Long)))
  }

  def validate(rio: Option[RentInOrder]): Option[ModelObject] = ???

  def put(c: RemoteService#PersistCallContext): Unit = {
    import c._
    val rio = apply(c.request)
    complete {
      validate(rio).getOrElse {
        RentInOrderCollection.update(rio.get)
        toMO(rio)
      }
    }
  }

}

object RentInOrderCollection extends RootCollection[RentInOrder] {
  override def name: String = "rent-in-order"
}
