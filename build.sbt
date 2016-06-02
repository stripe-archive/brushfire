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
  aggregate(brushfireTree, brushfireSerialization, brushfireTraining, brushfireScalding).
  settings(unidocSettings: _*).
  settings(unpublished: _*)

lazy val brushfireTree = project.
  in(file("brushfire-tree")).
  disablePlugins(sbtassembly.AssemblyPlugin)

lazy val brushfireTraining = project.
  in(file("brushfire-training")).
  dependsOn(brushfireTree).
  disablePlugins(sbtassembly.AssemblyPlugin)

lazy val brushfireSerialization = project.
  in(file("brushfire-serialization")).
  dependsOn(brushfireTree, brushfireTraining % "test->test;compile->compile").
  disablePlugins(sbtassembly.AssemblyPlugin)

lazy val brushfireScalding = project.
  in(file("brushfire-scalding")).
  dependsOn(brushfireTraining, brushfireSerialization)

lazy val brushfireFinatra = project.
  in(file("brushfire-finatra")).
  dependsOn(brushfireTraining)
