name := "brushfire-core"

libraryDependencies ++= {
  import Deps._
  Seq(
    algebirdCore,
    bijectionJson,
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
