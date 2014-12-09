name := "mercury-main"

version := "0.0.1-SNAPSHOT"

resolvers += "spray repo" at "http://repo.spray.io"

libraryDependencies ++= Seq(
  "org.scala-lang" % "scala-reflect" % scalaVersion.value withSources() withJavadoc(),
  "com.typesafe.akka" %% "akka-actor" % "2.3.7" withSources() withJavadoc(),
  "io.spray" %% "spray-can" % "1.3.2" withSources() withJavadoc(),
  "io.spray" %% "spray-routing" % "1.3.2" withSources() withJavadoc(),
  "io.spray" %% "spray-json" % "1.3.1" withSources() withJavadoc(),
  "mysql" % "mysql-connector-java" % "5.1.34",
  "com.typesafe.slick" %% "slick" % "2.1.0" withSources() withJavadoc(),
  "org.slf4j" % "slf4j-nop" % "1.6.4" withSources() withJavadoc(),
  "org.apache.commons" % "commons-dbcp2" % "2.0.1" withSources() withJavadoc()
)
