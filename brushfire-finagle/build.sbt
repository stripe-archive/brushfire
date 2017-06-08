name := "brushfire-finagle"

scalaVersion in ThisBuild := "2.11.11"
crossScalaVersions in ThisBuild := Seq("2.11.11")

libraryDependencies ++= Seq(Deps.finagle, Deps.bijectionJson)
MakeJar.settings
enablePlugins(JavaServerAppPackaging)

mappings in Universal := {
  val oldMappings = (mappings in Universal).value
  val oneJar = (assembly in Compile).value
  val filtered = oldMappings filter { case (_, name) => !name.endsWith(".jar") }
  filtered :+ (oneJar -> ("lib/" + oneJar.getName))
}

scriptClasspath := Seq((assemblyJarName in assembly).value)
