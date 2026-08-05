ThisBuild / organization := "dev.cascade"
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "3.3.8"

lazy val root = (project in file("."))
  .settings(
    name := "cascade",
    Compile / run / fork := true,
    Test / fork := true,
    Test / parallelExecution := false,
    scalacOptions ++= Seq(
      "-deprecation",
      "-feature",
      "-unchecked",
      "-Wunused:imports",
      "-Wunused:locals",
      "-Wunused:privates",
      "-Wvalue-discard"
    ),
    libraryDependencies ++= Seq(
      "org.scalameta" %% "munit" % "1.3.4" % Test,
      "org.apache.kafka" % "kafka-clients" % "4.3.1" % Test,
      "org.slf4j" % "slf4j-simple" % "2.0.17" % Test
    )
  )
