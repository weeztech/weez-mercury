package com.weez.mercury.product

import com.weez.mercury.common._
import com.github.nscala_time.time.Imports._

/**
 * 租入
 */
object RentInOrderService extends RemoteService {
  def getByID(id: Long)(implicit db: DBSessionQueryable): Option[RentInOrder] = RentInOrderCollection(id)

  val get: QueryCall = { c =>
    import c._
    complete(getByID(request.id).map(_.asModelObject).orNull)
  }
  val put: PersistCall = { c=>
    import c._
  }
  implicit final class ModelObject2Entity(private val self: ModelObject) extends AnyVal {
    def asRentInOrder:RentInOrder = {
???
      //RentInOrder()
    }
  }
  implicit final class RentInOrder2ModelObject(private val self: RentInOrder) extends AnyVal {
    def asModelObject(implicit db: DBSessionQueryable): ModelObject = {
      import self._
      ModelObject(
        "id" -> id,
        "datetime" -> datetime,
        "code" -> code,
        "warehouse" -> warehouse.asSimpleMO,
        "items" -> items.map(_.asModelObject)
      )
    }
  }

  implicit final class RentInOrderItem2ModelObject(private val self: RentInOrderItem) extends AnyVal {
    def asModelObject(implicit db: DBSessionQueryable): ModelObject = {
      import self._
      ModelObject(
        "product" -> product.asSimpleMO,
        "quantity" -> quantity,
        "serialNumbers" -> serialNumbers
      )
    }
  }

}

/**
 * 租入单子项
 * @param product 商品
 * @param quantity 数量
 * @param serialNumbers 物品唯一编号列表
 */
@packable
case class RentInOrderItem(product: Ref[Product],
                           quantity: Int,
                           serialNumbers: Seq[String])

/**
 * 租入单
 */
@packable
case class RentInOrder(datetime: DateTime,
                       code: String,
                       provider: Ref[Provider],
                       warehouse: Ref[Warehouse],
                       items: Seq[RentInOrderItem]) extends Entity {
}

object RentInOrderCollection extends RootCollection[RentInOrder] {
  override def name: String = "rent-in-order"
}