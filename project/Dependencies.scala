import sbt._

object Dependencies {

  val sparkVersion24 = "2.4.7"
  val sparkVersion3 = "3.1.2"
  // wait on https://github.com/scopt/scopt/issues/312
  val scoptVersion = "4.0.1"
  val scalatestVersion = "3.2.9"
  val scala212 = "2.12.15"
  val scala211 = "2.11.12"
  val scala213 = "2.13.7"

  val defaultSparkVersion = sparkVersion3
  val defaultScalaVersion = scala212

  lazy val scalaVers = sys.env.getOrElse("SCALA_VERSION", defaultScalaVersion)
  lazy val sparkVers = sys.env.getOrElse("SPARK_VERSION", defaultSparkVersion)

  val test = "org.scalatest" %% "scalatest" % scalatestVersion % Test
  val scopt = "com.github.scopt" %% "scopt" % scoptVersion % Compile

  def spark(scalaVersion: String) = {

    val deps = (version: String) => {
      Seq(
        "org.apache.spark" %% "spark-sql" % version % Provided,
        "org.apache.spark" %% "spark-core" % version % Provided
      )
    }

    val sparkVersEnv = sys.env.get("SPARK_VERSION")

    sparkVersEnv match {
      case Some(version) => deps(version)
      case None =>
        val sparkVers = if (scalaVersion == scala211) sparkVersion24 else sparkVersion3
        deps(sparkVers)
    }
  }
}
