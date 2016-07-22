name := "brushfire-scalding"

resolvers += Resolvers.conjars

libraryDependencies ++= Seq(
  Deps.hadoopClient,
  Deps.scaldingCore
)

mainClass := Some("com.twitter.scalding.Tool")

run in Compile <<= Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run))

runMain in Compile <<= Defaults.runMainTask(fullClasspath in Compile, runner in(Compile, run))

Publish.settings

MakeJar.settings
