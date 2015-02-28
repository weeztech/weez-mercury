package com.weez.mercury.common

import com.github.nscala_time.time.Imports._

object DTOHelper {

  import scala.language.dynamics

  final class RefsHelper(private val self: ModelObject) extends AnyVal with Dynamic {
    def selectDynamic[T <: Entity](name: String): Ref[T] = {
      if (!self.hasProperty(name)) {
        return RefEmpty
      }
      val rf: ModelObject = self.selectDynamic(name)
      if (rf eq null) {
        return RefEmpty
      }
      require((rf.$type: String) == "Ref", "$type must be 'Ref'")
      val id: Long = rf.id
      if (id == 0) {
        RefEmpty
      } else {
        RefSome[T](id)
      }
    }
  }

  final class SeqsHelper(private val self: ModelObject) extends AnyVal with Dynamic {
    @inline def selectDynamic(name: String): Seq[ModelObject] = {
      if (!self.hasProperty(name)) {
        return List.empty
      }
      self.selectDynamic[Seq[ModelObject]](name)
    }

  }

  final class OptionsHelper(private val self: ModelObject) extends AnyVal with Dynamic {
    @inline def selectDynamic(name: String): Option[ModelObject] = {
      if (!self.hasProperty(name)) {
        return None
      }
      Option(self.selectDynamic[ModelObject](name))
    }

  }

  implicit final class ModelObjectHelper(private val self: ModelObject) extends AnyVal with Dynamic {
    @inline def refs = new RefsHelper(self)

    @inline def seqs = new SeqsHelper(self)

    @inline def options = new OptionsHelper(self)
  }

  implicit final class RefHelper[E <: Entity](private val self: Ref[E]) extends AnyVal {
    def asMO()(implicit db: DBSessionQueryable): ModelObject = {
      asMO { (mo, o) =>}
    }

    def asMO(f: (ModelObject, E) => Unit)(implicit db: DBSessionQueryable): ModelObject = {
      val mo = new ModelObject(Map.empty[String, Any])
      val e = {
        val e = EntityCollections.getEntityO[E](self.id)
        if (e.isEmpty) null.asInstanceOf[E] else e.get
      }
      mo.$type = "Ref"
      if (e eq null) {
        mo.id = "0"
      } else {
        mo.id = e.id.toString
        f(mo, e)
      }
      mo
    }
  }

  implicit final class OptionHelper[T](private val self: Option[T]) extends AnyVal {
    def asMO(f: (ModelObject, T) => Unit)(implicit db: DBSessionQueryable): Any = {
      if (self.isEmpty) {
        null
      } else {
        val mo = new ModelObject(Map.empty[String, Any])
        f(mo, self.get)
        mo
      }
    }
  }

  implicit final class EntityHelper[E <: Entity](private val self: E) extends AnyVal {
    @inline
    def asMO(f: (ModelObject, E) => Unit): ModelObject = {
      val mo = new ModelObject(Map.empty)
      mo.$type = "Entity"
      mo.id = self.id.toString
      f(mo, self)
      mo
    }
  }

  implicit final class OptionEntityHelper[E <: Entity](private val self: Option[E]) extends AnyVal {
    @inline
    def asMO(f: (ModelObject, E) => Unit): ModelObject = {
      if (self.isEmpty) {
        return null
      }
      self.get.asMO(f)
    }
  }

  implicit final class SeqHelper[T](private val self: Seq[T]) extends AnyVal {
    @inline
    def asMO(f: (ModelObject, T) => Unit): Seq[ModelObject] = {
      self.map { item =>
        val mo = new ModelObject(Map.empty)
        f(mo, item)
        mo
      }
    }
  }

  implicit final class DateTimeHelper(private val self: DateTime) extends AnyVal {
    @inline
    def asMO: ModelObject = {
      val mo = new ModelObject(Map.empty)
      mo.$type = "Datetime"
      mo.value = self.toString
      mo
    }

    @inline def yearFloor = new DateTime(self.getYear, 1, 1, 0, 0)

    @inline def monthFloor = new DateTime(self.getYear, self.getMonthOfYear, 1, 0, 0)

    @inline def dayFloor = new DateTime(self.getYear, self.getMonthOfYear, self.getDayOfMonth, 0, 0)

    @inline def hourFloor = new DateTime(self.getYear, self.getMonthOfYear, self.getDayOfMonth, self.getHourOfDay, 0)

    @inline def minuteFloor = new DateTime(self.getYear, self.getMonthOfYear, self.getDayOfMonth, self.getHourOfDay, self.getMinuteOfHour)
  }

}
