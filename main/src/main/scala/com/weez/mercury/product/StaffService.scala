package com.weez.mercury.product

import com.weez.mercury.imports._

object StaffService extends RemoteService {
  /**
   * 获取Staff列表
   * ==response==
   * items: Seq <br>
   * - code: String 代码 <br>
   * - name: String 姓名 <br>
   * ==extra==
   * pager $pager <br>
   */
  def listStaffs: QueryCall = c => {
  }
}

@packable
case class Staff(code: String, name: String) extends Entity

object StaffCollection extends RootCollection[Staff] {
  def name = "staff"

  val byCode = defUniqueIndex("by-code", _.name)
}
