package com.weez.mercury

import java.io.File
import java.util.jar.JarFile

object ClassFinder {

  case class ScalaName(name: String, isModule: Boolean, isStatic: Boolean, isInternal: Boolean)


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
    val builder = Seq.newBuilder[String]
    ClassFinder.findClassesIn(file) { name =>
      builder += name
    }
    val classes = builder.result().sorted
    var parents: List[(String, String)] = Nil
    def findParent(originName: String) = {
      while (parents.nonEmpty && !originName.startsWith(parents.head._1 + "$"))
        parents = parents.tail
      parents.headOption
    }
    classes map { javaName =>
      import java.lang.reflect.Modifier

      val module = javaName.endsWith("$")
      val originName = if (module) javaName.substring(0, javaName.length - 1) else javaName
      val static = Modifier.isStatic(Class.forName(javaName).getModifiers)
      val scalaName =
        findParent(javaName) match {
          case Some((parentOriginName, parentScalaName)) =>
            if (originName == parentOriginName) {
              parentScalaName
            } else {
              parentScalaName + (if (static) "." else "#") +
                originName.substring(parentScalaName.length + 1)
            }
          case None => originName
        }
      val internal = scalaName.endsWith(".class")
      parents = (originName, scalaName) :: parents
      ScalaName(scalaName, module, static, internal)
    }
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
    val base = dir.getAbsolutePath() + "/"
    val dirs = scala.collection.mutable.Stack[File]()
    dirs.push(dir)
    while (dirs.nonEmpty) {
      for (file <- dirs.pop().listFiles()) {
        if (file.isDirectory)
          dirs.push(file)
        else if (file.getName.toLowerCase.endsWith(".class")) {
          val name = file.getAbsolutePath().substring(base.length)
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
