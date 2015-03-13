name := "brushfire-scalding"

resolvers += Resolvers.conjars

libraryDependencies ++= Seq(
  Deps.hadoopClient,
  Deps.scaldingCore
)

mainClass := Some("com.twitter.scalding.Tool")

Publish.settings

MakeJar.settings

