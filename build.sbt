organization := "com.weez"

name := "weez-mercury"

version := "0.0.1-SNAPSHOT"

scalaVersion in ThisBuild := "2.11.4"

scalacOptions in ThisBuild ++= Seq(
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

lazy val macros = project in file("macros")

lazy val main = project in file("main")

