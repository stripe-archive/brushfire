import sbt._
import sbt.Keys._

object Publish {
  val settings = Seq(
    homepage := Some(url("http://github.com/stripe/brushfire")),
    licenses += ("MIT License", url("http://www.opensource.org/licenses/mit-license.php")),
    publishMavenStyle := true,
    publishTo := {
      if (isSnapshot.value)
        sys.props.get("stripe.snapshots.url").map("stripe-nexus" at _)
      else
        Some("sonatype-nexus-staging" at "https://oss.sonatype.org/service/local/staging/deploy/maven2/")
    },
    publishArtifact in Test := false,
    pomExtra := (
      <developers>
        <developer>
          <name>Avi Bryant</name>
          <email>avi@stripe.com</email>
          <organization>Stripe</organization>
          <organizationUrl>https://stripe.com</organizationUrl>
        </developer>
      </developers>)
  )
}
