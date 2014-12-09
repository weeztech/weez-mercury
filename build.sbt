organization := "com.weez"

name := "weez-mercury"

version := "0.0.1-SNAPSHOT"

scalaVersion in ThisBuild := "2.11.4"

scalacOptions ++= Seq(
  "-feature",
  "-unchecked",
  "-deprecation",
  "-target:jvm-1.7"
)

publishTo in ThisBuild := {
  val nexus = "http://repo.slacktype.org/content/repositories/"
  if (isSnapshot.value)
    Some("snapshots" at nexus + "snapshots/")
  else
    Some("releases" at nexus + "releases/")
}

publishMavenStyle in ThisBuild := true

publishArtifact in Test := false

pomIncludeRepository in ThisBuild := { _ => false }

resolvers += "spray repo" at "http://repo.spray.io"

libraryDependencies ++= Seq(
  "org.scala-lang" %% "scala-pickling" % "0.9.0" withSources() withJavadoc(),
  "com.typesafe.akka" %% "akka-actor" % "2.3.7" withSources() withJavadoc(),
  "io.spray" %% "spray-can" % "1.3.2" withSources() withJavadoc(),
  "io.spray" %% "spray-routing" % "1.3.2" withSources() withJavadoc(),
  "com.typesafe.slick" %% "slick" % "2.1.0" withSources() withJavadoc(),
  "org.slf4j" % "slf4j-nop" % "1.6.4" withSources() withJavadoc(),
  "org.apache.commons" % "commons-dbcp2" % "2.0.1" withSources() withJavadoc()
)
