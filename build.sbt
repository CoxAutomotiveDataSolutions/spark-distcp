import sbt.Keys.{developers, fork, homepage, scalaVersion, scmInfo}
import sbt.url
import xerial.sbt.Sonatype._

lazy val scala212 = "2.12.8"
lazy val scala211 = "2.11.12"
lazy val supportedScalaVersions = List(scala212, scala211)

ThisBuild / scalaVersion := scala211
ThisBuild / organization := "com.coxautodata"

val sparkVersion = "2.4.3"
val scoptVersion = "3.5.0"
val scalatestVersion = "3.0.5"

lazy val compilerOptions = Seq(
  "-unchecked",
  "-feature",
  "-language:existentials",
  "-language:higherKinds",
  "-language:implicitConversions",
  "-language:postfixOps",
  "-deprecation",
  "-target:jvm-1.8",
  "-encoding",
  "utf8"
)

lazy val sparkdistcp = (project in file("."))
  .settings(
    name := "spark-distcp",
    fork in Test := true,
    scalacOptions ++= compilerOptions,
    crossScalaVersions := supportedScalaVersions,
    libraryDependencies += "org.scalatest" %% "scalatest" % scalatestVersion % Test,
    libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
    libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
    libraryDependencies += "com.github.scopt" %% "scopt" % scoptVersion % Compile,
    licenses := Seq(
      "APL2" -> url("http://www.apache.org/licenses/LICENSE-2.0.txt")
    ),
    description := "A re-implementation of Hadoop DistCP in Apache Spark",
    homepage := Some(
      url("https://github.com/CoxAutomotiveDataSolutions/spark-distcp")
    ),
    scmInfo := Some(
      ScmInfo(
        url("https://github.com/CoxAutomotiveDataSolutions/spark-distcp"),
        "scm:git@github.com:CoxAutomotiveDataSolutions/spark-distcp.git"
      )
    ),
    developers := List(
      Developer(
        id = "alexjbush",
        name = "Alex Bush",
        email = "alex.bush@coxauto.co.uk",
        url = url("https://alexbu.sh")
      ),
      Developer(
        id = "vavison",
        name = "Vicky Avison",
        email = "vicky.avison@coxauto.co.uk",
        url = url("https://coxautodata.com")
      )
    ),
    sonatypeProjectHosting := Some(
      GitHubHosting(
        "CoxAutomotiveDataSolutions",
        "spark-distcp",
        "alex.bush@coxauto.co.uk"
      )
    ),
    publishArtifact in Test := true,
    publishConfiguration := publishConfiguration.value
      .withOverwrite(isSnapshot.value),
    publishLocalConfiguration := publishLocalConfiguration.value
      .withOverwrite(isSnapshot.value)
  )
