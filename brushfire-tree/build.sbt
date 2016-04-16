name := "brushfire-tree"

libraryDependencies ++= {
  import Deps._
  Seq(
    algebirdCore,
    bonsai,
    scalaTest,
    scalaCheck
  )
}

Publish.settings
