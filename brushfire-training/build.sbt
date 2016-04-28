name := "brushfire-training"

libraryDependencies ++= {
  import Deps._
  Seq(
    algebirdCore,
    tDigest,
    scalaTest,
    scalaCheck
  )
}

Publish.settings
