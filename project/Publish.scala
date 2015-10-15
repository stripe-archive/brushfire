import sbt._
import sbt.Keys._

import aether.AetherKeys._
import com.typesafe.sbt.pgp.PgpKeys
import sbtrelease.ReleasePlugin.autoImport._
import sbtrelease.ReleaseStateTransformations._

object Publish {
  val nexus = "https://oss.sonatype.org/"
  def stripeSnapshotRepo = sys.props.get("stripe.snapshots.url").map("stripe-nexus" at _)
  def sonatypeSnapshotRepo = Some("Snapshots" at nexus + "content/repositories/snapshots")
  def sonatypeStagingRepo = Some("Releases" at nexus + "service/local/staging/deploy/maven2")

  val settings = Seq(
    releaseCrossBuild := true,
    releasePublishArtifactsAction := PgpKeys.publishSigned.value,
    publishMavenStyle := true,
    publish := {
      if (isSnapshot.value && stripeSnapshotRepo.isDefined) {
        aetherDeploy.value
      } else {
        publish.value
      }
    },
    publishTo := {
      if (isSnapshot.value) {
        stripeSnapshotRepo orElse sonatypeSnapshotRepo
      } else {
        sonatypeStagingRepo
      }
    },
    publishArtifact in Test := false,
    pomIncludeRepository := Function.const(false),
    homepage := Some(url("http://github.com/stripe/brushfire")),
    licenses += ("MIT License", url("http://www.opensource.org/licenses/mit-license.php")),
    pomExtra := (
      <scm>
        <url>git@github.com:stripe/brushfire.git</url>
        <connection>scm:git:git@github.com:stripe/brushfire.git</connection>
      </scm>
      <developers>
        <developer>
          <name>Avi Bryant</name>
          <email>avi@stripe.com</email>
          <organization>Stripe</organization>
          <organizationUrl>https://stripe.com</organizationUrl>
        </developer>
      </developers>),
    releaseProcess := Seq[ReleaseStep](
      checkSnapshotDependencies,
      inquireVersions,
      runClean,
      ReleaseStep(action = Command.process("package", _)),
      setReleaseVersion,
      commitReleaseVersion,
      tagRelease,
      ReleaseStep(action = Command.process("publishSigned", _)),
      setNextVersion,
      commitNextVersion,
      ReleaseStep(action = Command.process("sonatypeReleaseAll", _)),
      pushChanges)
  )
}
