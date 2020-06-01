name := "scio-koans"
description := "Scio Koans"

val scioVersion = "0.9.0"
val magnolifyVersion = "0.2.0"

val commonSettings = Seq(
  organization := "me.lyh",
  scalaVersion := "2.12.11",
  crossScalaVersions := Seq("2.12.11"),
  scalacOptions ++= Seq("-target:jvm-1.8", "-deprecation", "-feature", "-unchecked"),
)

val root: Project = Project(
  "scio-koans",
  file(".")
).settings(
  commonSettings,
  libraryDependencies ++= Seq(
    "com.spotify" %% "scio-core" % scioVersion,
    "com.spotify" %% "magnolify-cats" % magnolifyVersion,
  ),
)

