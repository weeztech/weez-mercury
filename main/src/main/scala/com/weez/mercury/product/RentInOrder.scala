package com.weez.mercury.product

import com.weez.mercury.common.ModelResponse
import com.weez.mercury.imports._
import com.github.nscala_time.time.Imports._
import com.weez.mercury.common.DTOHelper._

/**
 * 租入单子项
 * @param product 商品
 * @param quantity 数量
 * @param singleProducts 单品信息
 * @param totalValue 租入物品总价值
 * @param rentPrice 租金价格/每件日
 * @param stock 存入仓库
 */
@packable
case class RentInOrderItem(stock: Ref[Warehouse],
                           product: Ref[Product],
                           quantity: Int,
                           totalValue: Int,
                           singleProducts: Seq[SingleProductInfo],
                           rentPrice: Int,
                           remark: String)

/**
 * 租入单
 */
@packable
case class RentInOrder(datetime: DateTime,
                       number: String,
                       provider: Ref[Provider],
                       remark: String,
                       items: Seq[RentInOrderItem]) extends Entity

object RentInOrderHelper {

  def toMO(rentInOrder: Option[RentInOrder])(implicit db: DBSessionQueryable): ModelObject = {
    rentInOrder.asMO { (mo, o) =>
      mo.id = o.id.toString
      mo.datetime = o.datetime
      mo.number = o.number
      mo.remark = o.remark
      mo.provider = o.provider.asMO { (mo, o) =>
        mo.title = o.title
        mo.code = o.code
      }
      mo.items = o.items.asMO { (mo, o) =>
        mo.product = o.product.asMO { (mo, o) =>
          mo.title = o.title
          mo.code = o.code
        }
        mo.singleProductTrace = o.singleProducts.asMO { (mo, o) =>
          mo.serialNumber = o.serialNumber
          mo.prevBizRefID = o.prevBizRefID
          mo.nextBizRefID = o.nextBizRefID
        }
        mo.quantity = o.quantity
        mo.totalValue = o.totalValue
        mo.rentPrice = o.rentPrice
        mo.stock = o.stock.asMO { (mo, o) =>
          mo.title = o.title
          mo.code = o.code
        }
        mo.remark = o.remark
      }
    }
  }

  def apply(mo: ModelObject)(implicit db: DBSessionQueryable): Option[RentInOrder] = {
    if (mo eq null) {
      return None
    }
    val rio = RentInOrder(
      datetime = mo.dateTime,
      number = mo.number,
      provider = mo.refs.provider,
      remark = mo.remark,
      items = mo.seqs.items.map {
        mo =>
          RentInOrderItem(
            product = mo.refs.product,
            singleProducts = mo.seqs.singleProducts.map { mo =>
              SingleProductInfo(
                serialNumber = mo.serialNumber,
                prevBizRefID = mo.prevBizRefID,
                nextBizRefID = mo.nextBizRefID
              )
            },
            quantity = mo.quantity,
            rentPrice = mo.rentPrice,
            totalValue = mo.totalValue,
            stock = mo.refs.stock,
            remark = mo.remark
          )
      }
    )
    rio.id = mo.id
    Some(rio)
  }

  def get(c: RemoteService#QueryCallContext): Unit = {

    import c._

    complete(ModelResponse(toMO(RentInOrderCollection(request.id: Long))))
  }

  def validate(rio: Option[RentInOrder]): Option[ModelObject] = ???

  def put(c: RemoteService#PersistCallContext): Unit = {

    import c._

    val rio = apply(c.request)
    complete(ModelResponse {
      validate(rio).getOrElse {
        RentInOrderCollection.update(rio.get)
        toMO(rio)
      }
    })
  }
}

object RentInOrderCollection extends RootCollection[RentInOrder] {
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

/**
 * 租入归还单子项
 * @param product 商品
 * @param quantity 数量
 * @param singleProducts 单品信息
 * @param duration 租期
 * @param stock 归还仓库
 * @param totalValue 物品总价值
 * @param rentPrice 租金价格/每件日
 */
@packable
case class RentInReturnItem(stock: Ref[Warehouse],
                            product: Ref[Product],
                            quantity: Int,
                            totalValue: Int,
                            singleProducts: Seq[SingleProductInfo],
                            rentPrice: Int,
                            duration: Int,
                            remark: String)

/**
 * 租入归还单
 */
@packable
case class RentInReturn(datetime: DateTime,
                        number: String,
                        provider: Ref[Provider],
                        remark: String,
                        items: Seq[RentInReturnItem]) extends Entity

object RentInReturnCollection extends RootCollection[RentInReturn] {
  extractTo(ProductFlowDataBoard) { (o, db) =>
    o.items.map { item =>
      ProductFlow(o.id, o.datetime, item.stock.id, o.provider.id, item.product.id, item.quantity, item.totalValue)
    }
  }
  extractTo(SingleProductFlowDataBoard) { (o, db) =>
    o.items.flatMap { item =>
      val tv = item.totalValue / item.quantity
      item.singleProducts.map { s =>
        SingleProductFlow(o.id, o.datetime, item.stock.id, o.provider.id, item.product.id, s.serialNumber, tv, s.nextBizRefID != 0l)
      }
    }
  }
}