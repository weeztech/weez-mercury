name := "mercury-main"

version := "0.0.1-SNAPSHOT"

resolvers += "spray repo" at "http://repo.spray.io"

resolvers += Resolver.sonatypeRepo("releases")

libraryDependencies ++= Seq(
  "org.scala-lang" % "scala-reflect" % scalaVersion.value withSources() withJavadoc(),
  "com.typesafe.akka" %% "akka-actor" % "2.3.7" withSources() withJavadoc(),
  "io.spray" %% "spray-can" % "1.3.2" withSources() withJavadoc(),
  "io.spray" %% "spray-routing" % "1.3.2" withSources() withJavadoc(),
  "io.spray" %% "spray-json" % "1.3.1" withSources() withJavadoc(),
  "com.github.nscala-time" %% "nscala-time" % "1.6.0" withSources() withJavadoc(),
  "com.huaban" % "jieba-analysis" % "1.0.0" withSources() withJavadoc(),
  "com.github.stuxuhai" % "jpinyin" % "1.0" withSources() withJavadoc()
)
