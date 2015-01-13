name := "mercury-macors"

version := "0.0.1-SNAPSHOT"

libraryDependencies ++= Seq(
  "org.scala-lang" % "scala-reflect" % scalaVersion.value withSources() withJavadoc(),
  "org.scala-lang" % "scala-compiler" % scalaVersion.value withSources() withJavadoc()
)

