organization in ThisBuild := "com.stripe"

scalaVersion in ThisBuild := "2.11.5"

crossScalaVersions := Seq("2.10.4")

version in ThisBuild := "0.5.0-SNAPSHOT"

scalacOptions in ThisBuild ++= Seq(
  "-deprecation",
  "-feature",
  "-unchecked",
  "-optimize"
)

autoAPIMappings in ThisBuild := true

maxErrors in ThisBuild := 8

lazy val root = project.
  in(file(".")).
  aggregate(brushfireCore, brushfireScalding).
  settings(unidocSettings: _*)

lazy val brushfireCore = project.
  in(file("brushfire-core"))

lazy val brushfireScalding = project.
  in(file("brushfire-scalding")).
  dependsOn(brushfireCore)

lazy val brushfireFinatra = project.
  in(file("brushfire-finatra")).
  dependsOn(brushfireCore)
