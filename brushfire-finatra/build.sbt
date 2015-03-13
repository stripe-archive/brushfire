name := "brushfire-finatra"

resolvers += Resolvers.twttr

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
