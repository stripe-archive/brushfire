organization in ThisBuild := "com.stripe"

scalaVersion in ThisBuild := "2.11.5"

crossScalaVersions in ThisBuild := Seq("2.10.4", "2.11.5")

scalacOptions in ThisBuild ++= Seq(
  "-Yinline-warnings",
  "-deprecation",
  "-feature",
  "-unchecked",
  "-optimize"
)

autoAPIMappings in ThisBuild := true

maxErrors in ThisBuild := 8

val unpublished = Seq(publish := (), publishLocal := (), publishArtifact := false)

lazy val root = project.
  in(file(".")).
  aggregate(brushfireCore, brushfireScalding).
  settings(unidocSettings: _*).
  settings(unpublished: _*)

lazy val brushfireCore = project.
  in(file("brushfire-core")).
  disablePlugins(sbtassembly.AssemblyPlugin)

lazy val brushfireTraining = project.
  in(file("brushfire-training")).
  dependsOn(brushfireCore)

lazy val brushfireScalding = project.
  in(file("brushfire-scalding")).
  dependsOn(brushfireTraining)

lazy val brushfireFinatra = project.
  in(file("brushfire-finatra")).
  dependsOn(brushfireCore)
