package com.weez.mercury.product

import com.weez.mercury.imports.RemoteService
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

  /**
   * 新增Staff
   * ==request==
   * name: String 姓名 <br>
   * ==response==
   */
  def addStaff: PersistCall = c => {}

  /**
   * 获取code编码规则
   */
  def getCodeRule: QueryCall = c => {
  }

  /**
   * 设置code编码规则
   */
  def setCodeRule: PersistCall = c => {
  }

  def uploadImage: SimpleCall = c => {
    import c._
    // sdfjsdpfj
    saveUploadTempFile { (uc, path) =>
      // sdfsdf
      model()
    }
  }

  trait CodeGenerator {
    def next(): String
  }

  trait CodeRule {
    def generator(): CodeGenerator
  }

  object CodeRule {

    case class Constant(content: String) extends CodeRule {
      def generator() = new CodeGenerator {
        def next() = content
      }
    }

    case class Time(format: String) extends CodeRule {
      def generator() = new CodeGenerator {
        def next() = {
          DateTime.now.toString(format)
        }
      }
    }

    case class Counter(length: Int, start: Int, end: Int, hex: Boolean) extends CodeRule {
      def generator() = new CodeGenerator {
        var i = start

        def next() = {
          if (i < end) {
            val s = if (hex) f"$i%x" else f"$i%d"
            i += 1
            if (s.length < length) {
              "0" * (length - s.length) + s
            } else
              s
          } else
            throw new IllegalStateException("exceed limit")
        }
      }
    }

    case class Segment(rules: List[CodeRule]) extends CodeRule {
      def generator() = new CodeGenerator {
        val gs = rules map (_.generator())

        def next() = {
          val sb = new StringBuilder
          gs foreach (g => sb.append(g.next()))
          sb.toString()
        }
      }
    }

  }

}

@packable
case class Staff(code: String, name: String) extends Entity


object StaffCollection extends RootCollection[Staff] {
  def name = "staff"

  val byCode = defUniqueIndex("by-code", _.name)
}
