name := "mercury-macors"

version := "0.0.1-SNAPSHOT"

scalaVersion in ThisBuild := "2.11.4"

addCompilerPlugin("org.scalamacros" % "paradise" % "2.0.1" cross CrossVersion.full)

libraryDependencies ++= Seq(
  "org.scala-lang" % "scala-reflect" % scalaVersion.value withSources() withJavadoc(),
  "org.scala-lang" % "scala-compiler" % scalaVersion.value withSources() withJavadoc()
)

