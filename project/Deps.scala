import sbt._

object Resolvers {
  val conjars = "Conjars Repo" at "http://conjars.org/repo"
}

object Deps {
  object V {
    val algebird = "0.13.0"
    val jackson = "1.9.13"
    val bonsai = "0.3.0"
    val bijection = "0.9.5"
    val tDigest = "3.1"

    val hadoopClient = "2.5.2"
    val scalding = "0.17.4"
    val chill = "0.7.7"

    val finagle = "6.44.0"

    val scalaTest = "3.0.1"
    val scalaCheck = "1.13.4"
  }

  val algebirdCore   = "com.twitter"         %% "algebird-core"      % V.algebird
  val bijectionJson  = "com.twitter"         %% "bijection-json"     % V.bijection
  val bonsai         = "com.stripe"          %% "bonsai-core"        % V.bonsai
  val chillBijection = "com.twitter"         %% "chill-bijection"    % V.chill
  val jacksonMapper  = "org.codehaus.jackson" % "jackson-mapper-asl" % V.jackson
  val jacksonXC      = "org.codehaus.jackson" % "jackson-xc"         % V.jackson
  val jacksonJAXRS   = "org.codehaus.jackson" % "jackson-jaxrs"      % V.jackson
  val tDigest        = "com.tdunning"         % "t-digest"           % V.tDigest

  val hadoopClient   = "org.apache.hadoop"    % "hadoop-client"      % V.hadoopClient   % "provided"
  val scaldingCore   = "com.twitter"         %% "scalding-core"      % V.scalding

  val finagle        = "com.twitter"         %% "finagle-http"       % V.finagle

  val scalaTest      = "org.scalatest"       %% "scalatest"          % V.scalaTest      % "test"
  val scalaCheck     = "org.scalacheck"      %% "scalacheck"         % V.scalaCheck     % "test"
}
