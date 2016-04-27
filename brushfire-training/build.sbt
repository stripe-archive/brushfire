name := "brushfire-training"

libraryDependencies ++= {
  import Deps._
  Seq(
    algebirdCore,
    bijectionJson,
    bonsai,
    chillBijection,
    jacksonMapper,
    jacksonXC,
    jacksonJAXRS,
    tDigest,
    scalaTest,
    scalaCheck
  )
}

Publish.settings
