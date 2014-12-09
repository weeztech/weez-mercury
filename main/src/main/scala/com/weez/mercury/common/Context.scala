package com.weez.mercury.common

import scala.language.higherKinds
import scala.slick.lifted.CompiledFunction
import DB.driver.simple._

trait SimpleContext

trait QueryContext extends SimpleContext {
  protected[common] implicit val dbSession: Session

  def select[E, U, C[_]](query: Query[E, U, C]) = query.list

  def select[PU, RU](query: CompiledFunction[_, _, PU, _, RU])(params: PU): RU = query(params).run
}

trait PersistContext extends QueryContext {
  def insert[E, U, C[_]](query: Query[E, U, C], value: U) = query.insert(value)

  def update[E, U, C[_]](query: Query[E, U, C], value: U) = query.update(value)

  def delete[E <: Table[_], U, C[_]](query: Query[E, U, C], value: U) = query.delete

  def update[PU, R <: Query[_, _, C], C[_], RU](query: CompiledFunction[_, _, PU, R, C[RU]])(cond: PU)(set: RU): Int =
    query(cond).update(set)

  def delete[PU, R <: Query[_, _, C], C[_], RU](query: CompiledFunction[_, _, PU, R, C[RU]])(cond: PU): Int =
    query(cond).delete
}