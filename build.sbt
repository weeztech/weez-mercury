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

resolvers in ThisBuild += Resolver.sonatypeRepo("snapshots")

resolvers in ThisBuild += Resolver.sonatypeRepo("releases")

libraryDependencies in ThisBuild += compilerPlugin("org.scalamacros" % "paradise" % "2.0.1" cross CrossVersion.full)

lazy val macros = project in file("macros")

lazy val main = project in file("main") dependsOn macros
