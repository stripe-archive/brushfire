import sbt._
import sbt.Keys._
import com.typesafe.sbt.pgp.PgpKeys._

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
    useGpg := true,
    publishArtifact in Test := false,
    pomIncludeRepository := { _ => false },
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
      </developers>)
  )
}
