name := "brushfire-finatra"

// Required for org.apache.thrift#libthrift;0.5.0, used by Finagle.
resolvers += "Twitter" at "http://maven.twttr.com"

libraryDependencies += Deps.finatra

Publish.settings

MakeJar.settings

enablePlugins(JavaServerAppPackaging)

mappings in Universal := {
  val oldMappings = (mappings in Universal).value
  val oneJar = (assembly in Compile).value
  val filtered = oldMappings filter { case (_, name) => !name.endsWith(".jar") }
  filtered :+ (oneJar -> ("lib/" + oneJar.getName))
}

scriptClasspath := Seq((jarName in assembly).value)
