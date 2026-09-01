ThisBuild / organization := "dev.cascade"
lazy val cascadeVersion = {
  val value = IO.read(file("VERSION")).trim
  require(value.matches("[0-9]+\\.[0-9]+\\.[0-9]+(?:-[0-9A-Za-z.-]+)?"), s"invalid Cascade version: $value")
  value
}
ThisBuild / version := cascadeVersion
ThisBuild / scalaVersion := "3.3.8"

lazy val stage = taskKey[File]("Build the dependency-complete runtime tree used by the container image")

lazy val root = (project in file("."))
  .settings(
    name := "cascade",
    Compile / mainClass := Some("cascade.Main"),
    Compile / run / fork := true,
    Test / fork := true,
    Test / parallelExecution := false,
    Test / javaOptions += "-Dorg.slf4j.simpleLogger.defaultLogLevel=warn",
    scalacOptions ++= Seq(
      "-deprecation",
      "-feature",
      "-unchecked",
      "-Wunused:imports",
      "-Wunused:locals",
      "-Wunused:privates",
      "-Wvalue-discard"
    ),
    Compile / sourceGenerators += Def.task {
      val output = (Compile / sourceManaged).value / "cascade" / "BuildInfo.scala"
      IO.write(
        output,
        s"""|package cascade
            |
            |object BuildInfo:
            |  val Version: String = "$cascadeVersion"
            |""".stripMargin
      )
      Seq(output)
    }.taskValue,
    libraryDependencies ++= Seq(
      "org.scalameta" %% "munit" % "1.3.4" % Test,
      "org.apache.kafka" % "kafka-clients" % "4.3.1" % Test,
      "org.slf4j" % "slf4j-simple" % "2.0.17" % Test
    ),
    stage := {
      val destination = target.value / "docker-stage"
      val libraryDirectory = destination / "lib"
      val applicationJar = (Compile / packageBin).value
      val dependencyJars = (Compile / dependencyClasspath).value.map(_.data).filter(_.isFile)
      val runtimeJars = applicationJar +: dependencyJars
      val duplicateNames = runtimeJars.groupBy(_.getName).collect { case (name, files) if files.size > 1 => name }
      if (duplicateNames.nonEmpty) {
        sys.error(s"runtime dependency filenames collide: ${duplicateNames.toVector.sorted.mkString(", ")}")
      }
      IO.delete(destination)
      IO.createDirectory(libraryDirectory)
      runtimeJars.foreach(file => IO.copyFile(file, libraryDirectory / file.getName, preserveLastModified = true))
      streams.value.log.info(s"staged ${runtimeJars.size} runtime jars in ${destination.getAbsolutePath}")
      destination
    }
  )
