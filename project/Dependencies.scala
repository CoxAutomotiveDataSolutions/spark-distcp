import sbt._

object Dependencies {

  val sparkVersion24 = "2.4.7"
  val sparkVersion3 = "3.0.1"
  val scoptVersion = "3.5.0"
  val scalatestVersion = "3.2.3"
  val scala212 = "2.12.12"
  val scala211 = "2.11.12"

  val test = "org.scalatest" %% "scalatest" % scalatestVersion % Test
  val scopt = "com.github.scopt" %% "scopt" % scoptVersion % Compile

  def spark(scalaVersion: String) = {
    val sparkVers = if (scalaVersion == scala211) sparkVersion24 else sparkVersion3
    Seq(
      "org.apache.spark" %% "spark-sql" % sparkVers % Provided,
      "org.apache.spark" %% "spark-core" % sparkVers % Provided
    )
  }
}
