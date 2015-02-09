package com.weez.mercury.common

import java.io.File
import java.util.jar.JarFile

import akka.event.LoggingAdapter

import scala.annotation.StaticAnnotation
import scala.reflect.runtime.universe._

class collect extends StaticAnnotation

object ClassFinder {
  private val collectType = typeOf[collect]

  def collectTypes(log: LoggingAdapter) = {
    import scala.collection.mutable
    val watchedTypes = mutable.ArrayBuffer[Type]()
    val collected = mutable.Map[String, mutable.ArrayBuffer[Symbol]]()
    val start = System.nanoTime()
    val mirror = runtimeMirror(this.getClass.getClassLoader)
    val names = scalaNamesIn(classpath.filter { f =>
      f.isDirectory || f.getName.contains("weez")
    }).filter(_.isStatic)
    names.withFilter(!_.isModule) foreach { scalaName =>
      def check(c: Tree): Unit = {
        var t = c
        var stop = false
        while (!stop) {
          t match {
            case Select(x, _) => t = x
            case Ident(_) => stop = true
            case _ =>
              throw new IllegalArgumentException(s"expect stable object: file ${t.pos.source.file.path}, line ${t.pos.line}, col ${t.pos.column}")
          }
        }
      }
      val symbol = mirror.staticClass(scalaName.name)
      if (symbol.isTrait || symbol.isAbstract) {
        symbol.annotations.find(_.tree.tpe =:= collectType) match {
          case Some(_) =>
            watchedTypes.append(symbol.toType.erasure)
            collected.put(symbol.fullName, mutable.ArrayBuffer())
            log.debug("found collecting type: {}", symbol.fullName)
          case _ =>
        }
      }
    }
    names foreach { scalaName =>
      var symbol: Symbol = null
      var tpe: Type = null
      if (scalaName.isModule) {
        val moduleSymbol = mirror.staticModule(scalaName.name)
        symbol = moduleSymbol
        tpe = moduleSymbol.moduleClass.asClass.toType
      } else {
        val c = mirror.staticClass(scalaName.name)
        symbol = c
        tpe = c.toType
      }
      watchedTypes foreach { t =>
        if (tpe <:< t && !(tpe =:= t)) {
          collected(t.typeSymbol.fullName).append(symbol)
          log.debug("collect type: {} -> {}", symbol.fullName, t.typeSymbol.fullName)
        }
      }
    }
    collected
  }

  case class ScalaName(name: String, isModule: Boolean, isStatic: Boolean)

  def classpath = System.getProperty("java.class.path").
    split(File.pathSeparator).
    map(s => if (s.trim.length == 0) "." else s).
    map(new File(_)).
    toList

  def scalaNamesIn(files: Seq[File]): Seq[ScalaName] = {
    val builder = Seq.newBuilder[ScalaName]
    files foreach { f =>
      builder ++= scalaNamesIn(f)
    }
    builder.result()
  }

  def scalaNamesIn(file: File): Seq[ScalaName] = {
    val classes = classesIn(file).sorted
    var parents: List[(String, String)] = Nil
    def findParent(originName: String) = {
      while (parents.nonEmpty && !originName.startsWith(parents.head._1 + "$"))
        parents = parents.tail
      parents.headOption
    }
    val builder = Seq.newBuilder[ScalaName]
    classes foreach { javaName =>
      import java.lang.reflect.Modifier
      if (!javaName.endsWith("$class")) {
        val module = javaName.endsWith("$")
        val originName = if (module) javaName.substring(0, javaName.length - 1) else javaName
        var static = true
        var scalaName: String = null
        findParent(javaName) match {
          case Some((parentOriginName, parentScalaName)) =>
            if (originName == parentOriginName) {
              if (parents.length > 1)
                static = Modifier.isStatic(Class.forName(javaName).getModifiers)
              scalaName = parentScalaName
            } else {
              static = Modifier.isStatic(Class.forName(javaName).getModifiers)
              scalaName = parentScalaName + (if (static) "." else "#") +
                originName.substring(parentScalaName.length + 1)
            }
          case None =>
            scalaName = originName
        }
        parents = (originName, scalaName) :: parents
        if (!scalaName.contains("$")) {
          builder += ScalaName(scalaName, module, static)
        }
      }
    }
    builder.result()
  }

  def classesIn(file: File) = {
    val builder = Seq.newBuilder[String]
    findClassesIn(file) { name =>
      builder += name
    }
    builder.result()
  }

  def findClassesIn(file: File)(f: String => Unit): Unit = {
    val name = file.getPath.toLowerCase
    if (name.endsWith(".jar"))
      findClassesInJar(file)(f)
    else if (file.isDirectory)
      findClassesInDir(file)(f)
    else
      throw new IllegalArgumentException
  }

  def findClassesIn(files: Seq[File])(f: String => Unit): Unit = {
    files foreach (findClassesIn(_)(f))
  }

  def findClassesInDir(dir: File)(f: String => Unit) = {
    val base = dir.getAbsolutePath + "/"
    val dirs = scala.collection.mutable.Stack[File]()
    dirs.push(dir)
    while (dirs.nonEmpty) {
      for (file <- dirs.pop().listFiles()) {
        if (file.isDirectory)
          dirs.push(file)
        else if (file.getName.toLowerCase.endsWith(".class")) {
          val name = file.getAbsolutePath.substring(base.length)
          f(name.substring(0, name.length - ".class".length).replace('/', '.'))
        }
      }
    }
  }

  def findClassesInJar(file: File)(f: String => Unit) = {
    val jar = new JarFile(file)
    try {
      val itor = jar.entries
      while (itor.hasMoreElements) {
        val e = itor.nextElement()
        val name = e.getName
        if (!e.isDirectory && name.toLowerCase.endsWith(".class")) {
          f(name.substring(0, name.length - ".class".length).replace('/', '.'))
        }
      }
    } finally {
      jar.close()
    }
  }
}
