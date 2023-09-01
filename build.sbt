ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.11"

val SparkVersion = "3.3.0"

lazy val root = (project in file("."))
  .settings(
    name := "interview",
    libraryDependencies ++= Dependencies.list
  )
