import sbt._
import sbt.Keys._
import sbtassembly.AssemblyKeys._
import sbtassembly._

object MakeJar {
  val settings = Seq(
    assemblyJarName in assembly := name.value + "-" + version.value + "-jar-with-dependencies.jar",
    assemblyMergeStrategy in assembly := {
      val defaultStrategy = (assemblyMergeStrategy in assembly).value

      { 
        case PathList(ps @ _*) if ps.last endsWith ".class" => MergeStrategy.first
        case path => defaultStrategy(path)
      }
    }
  )
}
