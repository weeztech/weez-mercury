name := "mercury-macors"

version := "0.0.1-SNAPSHOT"

libraryDependencies ++= Seq(
  "org.scala-lang" % "scala-reflect" % scalaVersion.value withSources() withJavadoc(),
  "org.scala-lang" % "scala-compiler" % scalaVersion.value withSources() withJavadoc(),
  "com.typesafe.slick" %% "slick" % "2.1.0" withSources() withJavadoc(),
  "org.slf4j" % "slf4j-nop" % "1.6.4" withSources() withJavadoc(),
  "io.spray" %% "spray-json" % "1.3.1" withSources() withJavadoc(),
  "com.chuusai" %% "shapeless" % "1.2.4" withSources() withJavadoc()
)

