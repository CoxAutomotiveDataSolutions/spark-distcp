import sbt.Keys.{developers, fork, homepage, scalaVersion, scmInfo}
import sbt.url
import xerial.sbt.Sonatype._
import Dependencies.{scopt, spark, test}

lazy val scala212 = Dependencies.scala212
lazy val scala211 = Dependencies.scala211
lazy val scala213 = Dependencies.scala213
lazy val supportedScalaVersions = List(scala213, scala212, scala211)

ThisBuild / scalaVersion := Dependencies.scalaVers
ThisBuild / organization := "com.coxautodata"

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

addCommandAlias("ci", ";+compile ;+test")

lazy val sparkdistcp = (project in file("."))
  .settings(
    name := "spark-distcp",
    Test / fork := true,
    scalacOptions ++= compilerOptions,
    crossScalaVersions := supportedScalaVersions,
    libraryDependencies += test,
    libraryDependencies += scopt,
    libraryDependencies ++= spark(scalaVersion.value),
    libraryDependencies += "org.scala-lang.modules" %% "scala-collection-compat" % Dependencies.collectionCompat,
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
      ),
      Developer(
        id = "jamesfielder",
        name = "James Fielder",
        email = "james@fielder.dev",
        url = url("https://james.fielder.dev")
      )
    ),
    sonatypeProjectHosting := Some(
      GitHubHosting(
        "CoxAutomotiveDataSolutions",
        "spark-distcp",
        "alex.bush@coxauto.co.uk"
      )
    ),
    Test / publishArtifact := true,
    publishConfiguration := publishConfiguration.value
      .withOverwrite(isSnapshot.value),
    publishLocalConfiguration := publishLocalConfiguration.value
      .withOverwrite(isSnapshot.value)
  )
