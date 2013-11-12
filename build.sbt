name := """scalasyd-first-akka-cluster"""

version := "0.1"

scalaVersion := "2.10.2"

libraryDependencies ++= Seq(
  "com.github.nscala-time" %% "nscala-time" % "0.4.0",
  "com.typesafe.akka" %% "akka-contrib" % "2.2.1",
  "com.typesafe.akka" %% "akka-testkit" % "2.2.1",
  "org.scalaz" %% "scalaz-core" % "7.0.4",
  "org.scalaz" %% "scalaz-effect" % "7.0.4",
  "org.specs2" %% "specs2" % "2.1.1" % "test",
  "org.scalatest" % "scalatest_2.10" % "1.9.1" % "test")
