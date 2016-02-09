name := "brushfire-scalding"

resolvers += Resolvers.conjars

libraryDependencies ++= Seq(
  Deps.hadoopClient % "provided",
  Deps.scaldingCore
)

Publish.settings
