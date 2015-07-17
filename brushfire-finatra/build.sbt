name := "brushfire-finatra"

scalaVersion in ThisBuild := "2.10.4"

crossScalaVersions in ThisBuild := Seq("2.10.4", "2.11.5")

// Required for org.apache.thrift#libthrift;0.5.0, used by Finagle.
resolvers += "Twitter" at "https://maven.twttr.com"

libraryDependencies += Deps.finatra

MakeJar.settings

enablePlugins(JavaServerAppPackaging)

mappings in Universal := {
  val oldMappings = (mappings in Universal).value
  val oneJar = (assembly in Compile).value
  val filtered = oldMappings filter { case (_, name) => !name.endsWith(".jar") }
  filtered :+ (oneJar -> ("lib/" + oneJar.getName))
}

scriptClasspath := Seq((jarName in assembly).value)
