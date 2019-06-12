package com.coxautodata.utils

import java.net.URI

import PathUtils._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.scalatest.{FunSpec, Matchers}

class TestPathUtils extends FunSpec with Matchers {

  describe("pathToQualifiedPath") {

    it("qualify a path with the default fs") {
      val path = new Path("/test")

      path.toString should be("/test")
      pathToQualifiedPath(new Configuration(), path)
        .toString should be("file:/test")
    }

    it("not change an existing uri") {
      val path = new Path("hdfs:/test")

      path.toString should be("hdfs:/test")
      pathToQualifiedPath(new Configuration(), path)
        .toString should be("hdfs:/test")
    }

  }

  describe("sourcePathToDestinationPath") {

    it("non-update/non-overwrite behaviour") {

      // Source is subdirectory
      sourceURIToDestinationURI(
        new Path("hdfs://nn1:8020/source/first/1").toUri,
        new Path("hdfs://nn1:8020/source/first").toUri,
        new Path("hdfs://nn2:8020/target").toUri,
        updateOverwritePathBehaviour = false) should be(
        new Path("hdfs://nn2:8020/target/first/1").toUri
      )

      // Source is root
      sourceURIToDestinationURI(
        new Path("hdfs://nn1:8020/first/1").toUri,
        new Path("hdfs://nn1:8020/").toUri,
        new Path("hdfs://nn2:8020/target").toUri,
        updateOverwritePathBehaviour = false) should be(
        new Path("hdfs://nn2:8020/target/first/1").toUri
      )

    }

    it("update/overwrite behaviour") {

      // Source is subdirectory
      sourceURIToDestinationURI(
        new Path("hdfs://nn1:8020/source/first/1").toUri,
        new Path("hdfs://nn1:8020/source/first").toUri,
        new Path("hdfs://nn2:8020/target").toUri,
        updateOverwritePathBehaviour = true) should be(
        new Path("hdfs://nn2:8020/target/1").toUri
      )

      // Source is root
      sourceURIToDestinationURI(
        new Path("hdfs://nn1:8020/first/1").toUri,
        new Path("hdfs://nn1:8020/").toUri,
        new Path("hdfs://nn2:8020/target").toUri,
        updateOverwritePathBehaviour = true) should be(
        new Path("hdfs://nn2:8020/target/first/1").toUri
      )

    }

  }

  describe("uriIsChild") {

    it("check whether path is a child") {

      uriIsChild(new URI("file:/test/folder"), new URI("file:/test/folder/child")) should be(true)
      uriIsChild(new URI("file:/test/folder"), new URI("file:/test/folder")) should be(true)
      uriIsChild(new URI("file:/test/folder"), new URI("file:/test")) should be(false)
      uriIsChild(new URI("file:/test/folder"), new URI("hdfs:/test/folder/child")) should be(false)
      uriIsChild(new URI("file:/test/folder"), new URI("hdfs:/test/folder.file")) should be(false)

    }

    it("fail on non-absolute URIs") {

      intercept[RuntimeException] {
        uriIsChild(new URI("file:/test/folder"), new URI("/test/folder/child"))
      }

      intercept[RuntimeException] {
        uriIsChild(new URI("/test/folder"), new URI("file:/test/folder/child"))
      }

      intercept[RuntimeException] {
        uriIsChild(new URI("file:/test/folder"), new URI("file:test/folder/child"))
      }

      intercept[RuntimeException] {
        uriIsChild(new URI("file:test/folder"), new URI("file:/test/folder/child"))
      }

    }

  }

}
