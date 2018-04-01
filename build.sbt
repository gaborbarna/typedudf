import sbt.Keys._
import sbt._

name := "typedudf"

organization := "com.github.lesbroot"

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

publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value) Some("snapshots" at nexus + "content/repositories/snapshots")
  else Some("releases"  at nexus + "service/local/staging/deploy/maven2")
}

licenses := Seq("BSD-style" -> url("http://www.opensource.org/licenses/bsd-license.php"))

homepage := Some(url("https://github.com/lesbroot/typedudf"))

scmInfo := Some(
  ScmInfo(
    url("https://github.com/lesbroot/typedudf"),
    "scm:git@github.com:lesbroot/typedudf.git"
  )
)

developers := List(
  Developer(
    id    = "lesbroot",
    name  = "Gabor Barna",
    email = "mr.barna.gabor@gmail.com",
    url   = url("https://github.com/lesbroot")
  )
)

publishMavenStyle := true
