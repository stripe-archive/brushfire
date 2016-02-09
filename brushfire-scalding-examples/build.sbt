name := "brushfire-scalding-examples"

resolvers += Resolvers.conjars

libraryDependencies ++= Seq(
  Deps.hadoopClient,
  Deps.scaldingCore
)

mainClass := Some("com.twitter.scalding.Tool")

MakeJar.settings
