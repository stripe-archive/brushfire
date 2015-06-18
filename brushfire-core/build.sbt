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
    scalaTest,
    scalaCheck
  )
}

Publish.settings
