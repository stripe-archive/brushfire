name := "brushfire-core"

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
    spire,
    scalaTest,
    scalaCheck
  )
}

Publish.settings
