import sbt.Keys._
import sbt._

name := "typedudf"

organization := "com.lesbroot"

scalaVersion := "2.11.12"

val sparkVersion = "2.2.+"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
  "com.chuusai" %% "shapeless" % "2.3.+",
  "org.scalatest" %% "scalatest" % "[3, 3.1)" % Test
)

scalacOptions ++= Seq(
  "-deprecation",
  "-encoding", "UTF-8",
  "-feature",
  "-language:higherKinds",
  "-unchecked",
  "-Yno-adapted-args",
  "-Ywarn-dead-code",
  "-Ywarn-numeric-widen",
  "-Xfuture",
  "-Xlint"
)
