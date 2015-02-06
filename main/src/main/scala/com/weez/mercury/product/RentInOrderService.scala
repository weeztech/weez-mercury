package com.weez.mercury.product

import com.weez.mercury.imports._
import com.github.nscala_time.time.Imports._
import com.weez.mercury.common.DTOHelper._

/**
 * 租入
 */
object RentInOrderService extends RemoteService {
  import MasterDataHelper._

  def toMO(rentInOrder: Option[RentInOrder])(implicit db: DBSessionQueryable): ModelObject = {
    rentInOrder.asMO { (mo, o) =>
      mo.id = o.id.toString
      mo.datetime = o.datetime
      mo.code = o.code
      mo.warehouse = o.warehouse.codeTitleAsMO
      mo.items = o.items.asMO { (mo, o) =>
        mo.product = o.product.codeTitleAsMO
        mo.quantity = o.quantity
        mo.price = o.price
        mo.serialNumbers = o.singleProducts.asMO { (mo, o) =>
          mo.serialNumber = o.serialNumber
          mo.remark = o.remark
        }
      }
    }
  }

  def parseRentInOrder(mo: ModelObject)(implicit db: DBSessionQueryable): Option[RentInOrder] = {
    if (mo eq null) {
      return None
    }
    val rio = RentInOrder(
      datetime = mo.dateTime,
      code = mo.code,
      provider = mo.refs.provider,
      warehouse = mo.refs.warehouse,
      items = mo.seqs.items.map { mo =>
        RentInOrderItem(
          product = mo.refs.product,
          quantity = mo.quantity,
          price = mo.price,
          singleProducts = mo.seqs.singleProducts.map { mo =>
            SingleProduct(
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

  def validate(rio: Option[RentInOrder])(implicit db: DBSessionQueryable): Option[ModelObject] = {
    ???
  }

  val put: PersistCall = { c =>
    import c._
    val rio = parseRentInOrder(c.request)
    complete {
      validate(rio).getOrElse {
        RentInOrderCollection.update(rio.get)
        toMO(rio)
      }
    }
  }

  val get: QueryCall = { c =>
    import c._
    complete(toMO(RentInOrderCollection(request.id: Long)))
  }
}

@packable
case class SingleProduct(serialNumber: String, remark: String)

/**
 * 租入单子项
 * @param product 商品
 * @param quantity 数量
 * @param singleProducts 单品信息 serialNumber -> remark
 */
@packable
case class RentInOrderItem(product: Ref[Product],
                           quantity: Int,
                           price: Int,
                           singleProducts: Seq[SingleProduct])

/**
 * 租入单
 */
@packable
case class RentInOrder(datetime: DateTime,
                       code: String,
                       provider: Ref[Provider],
                       warehouse: Ref[Warehouse],
                       items: Seq[RentInOrderItem]) extends Entity

object RentInOrderCollection extends RootCollection[RentInOrder] {
  override def name: String = "rent-in-order"
}