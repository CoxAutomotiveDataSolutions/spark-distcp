package com.coxautodata.utils

import com.coxautodata.TestSpec
import com.coxautodata.utils.FileListUtils._
import org.apache.hadoop.fs.Path
import org.apache.spark.{SparkConf, SparkContext}

class TestFileListUtils extends TestSpec {

  describe("listFiles") {

    it("Non-existing folder") {
      intercept[java.io.FileNotFoundException] {
        listFiles(
          localFileSystem,
          new Path(testingBaseDirPath, "src"),
          10,
          false,
          List.empty,
          FailMissingDirectoryAction
        )
      }
    }

    it("Empty folder") {
      localFileSystem.mkdirs(new Path(testingBaseDirPath, "src"))
      listFiles(
        localFileSystem,
        new Path(testingBaseDirPath, "src"),
        10,
        false,
        List.empty,
        FailMissingDirectoryAction
      ) should contain theSameElementsAs Seq.empty
    }

    it("Empty folder with include root") {
      localFileSystem.mkdirs(new Path(testingBaseDirPath, "src"))
      listFiles(
        localFileSystem,
        new Path(testingBaseDirPath, "src"),
        10,
        true,
        List.empty,
        FailMissingDirectoryAction
      )
        .map(f =>
          (fileStatusToResult(f._1), f._2.map(fileStatusToResult))
        ) should contain theSameElementsAs Seq(
        (
          FileListing(new Path(testingBaseDirPath, "src").toString, None),
          List()
        )
      )
    }

    it("Single file with include root") {
      localFileSystem.mkdirs(new Path(testingBaseDirPath, "src"))
      createFile(new Path("src/file"), "a".getBytes)
      listFiles(
        localFileSystem,
        new Path(testingBaseDirPath, "src"),
        10,
        true,
        List.empty,
        FailMissingDirectoryAction
      )
        .map(f =>
          (fileStatusToResult(f._1), f._2.map(fileStatusToResult))
        ) should contain theSameElementsAs Seq(
        (
          FileListing(new Path(testingBaseDirPath, "src").toString, None),
          List()
        ),
        (
          FileListing(
            new Path(testingBaseDirPath, "src/file").toString,
            Some(1)
          ),
          List(FileListing(new Path(testingBaseDirPath, "src").toString, None))
        )
      )
    }

    it("Should list all files in folder") {

      val input = List(
        "src/1.file",
        "src/2.file",
        "src/3.file",
        "src/sub1/1.file",
        "src/sub1/2.file",
        "src/sub1/3.file",
        "src/sub2/1.file",
        "src/sub2/2.file",
        "src/sub2/3.file",
        "src/sub2/subsub1/1.file"
      )

      input.foreach(f => createFile(new Path(f), f.getBytes))

      listFiles(
        localFileSystem,
        new Path(testingBaseDirPath, "src"),
        10,
        false,
        List.empty,
        FailMissingDirectoryAction
      )
        .map(f =>
          (fileStatusToResult(f._1), f._2.map(fileStatusToResult))
        ) should contain theSameElementsAs Seq(
        (
          FileListing(
            new Path(testingBaseDirPath, "src/1.file").toString,
            Some(10)
          ),
          List()
        ),
        (
          FileListing(
            new Path(testingBaseDirPath, "src/2.file").toString,
            Some(10)
          ),
          List()
        ),
        (
          FileListing(
            new Path(testingBaseDirPath, "src/3.file").toString,
            Some(10)
          ),
          List()
        ),
        (
          FileListing(new Path(testingBaseDirPath, "src/sub1").toString, None),
          List()
        ),
        (
          FileListing(new Path(testingBaseDirPath, "src/sub2").toString, None),
          List()
        ),
        (
          FileListing(
            new Path(testingBaseDirPath, "src/sub2/subsub1").toString,
            None
          ),
          List(
            FileListing(new Path(testingBaseDirPath, "src/sub2").toString, None)
          )
        ),
        (
          FileListing(
            new Path(testingBaseDirPath, "src/sub1/1.file").toString,
            Some(15)
          ),
          List(
            FileListing(new Path(testingBaseDirPath, "src/sub1").toString, None)
          )
        ),
        (
          FileListing(
            new Path(testingBaseDirPath, "src/sub1/2.file").toString,
            Some(15)
          ),
          List(
            FileListing(new Path(testingBaseDirPath, "src/sub1").toString, None)
          )
        ),
        (
          FileListing(
            new Path(testingBaseDirPath, "src/sub1/3.file").toString,
            Some(15)
          ),
          List(
            FileListing(new Path(testingBaseDirPath, "src/sub1").toString, None)
          )
        ),
        (
          FileListing(
            new Path(testingBaseDirPath, "src/sub2/1.file").toString,
            Some(15)
          ),
          List(
            FileListing(new Path(testingBaseDirPath, "src/sub2").toString, None)
          )
        ),
        (
          FileListing(
            new Path(testingBaseDirPath, "src/sub2/2.file").toString,
            Some(15)
          ),
          List(
            FileListing(new Path(testingBaseDirPath, "src/sub2").toString, None)
          )
        ),
        (
          FileListing(
            new Path(testingBaseDirPath, "src/sub2/3.file").toString,
            Some(15)
          ),
          List(
            FileListing(new Path(testingBaseDirPath, "src/sub2").toString, None)
          )
        ),
        (
          FileListing(
            new Path(testingBaseDirPath, "src/sub2/subsub1/1.file").toString,
            Some(23)
          ),
          List(
            FileListing(
              new Path(testingBaseDirPath, "src/sub2").toString,
              None
            ),
            FileListing(
              new Path(testingBaseDirPath, "src/sub2/subsub1").toString,
              None
            )
          )
        )
      )

    }

    it("Should list all files in folder with a filter") {

      val input = List(
        "src/1.file",
        "src/2.file",
        "src/3.file",
        "src/sub1/1.file",
        "src/sub1/2.file",
        "src/sub1/3.file",
        "src/sub2/1.file",
        "src/sub2/2.file",
        "src/sub2/3.file",
        "src/sub2/subsub1/1.file"
      )

      input.foreach(f => createFile(new Path(f), f.getBytes))

      listFiles(
        localFileSystem,
        new Path(testingBaseDirPath, "src"),
        10,
        false,
        List(""".*/1\.file$""".r, """.*/3\.file$""".r),
        FailMissingDirectoryAction
      )
        .map(f =>
          (fileStatusToResult(f._1), f._2.map(fileStatusToResult))
        ) should contain theSameElementsAs Seq(
        (
          FileListing(
            new Path(testingBaseDirPath, "src/2.file").toString,
            Some(10)
          ),
          List()
        ),
        (
          FileListing(new Path(testingBaseDirPath, "src/sub1").toString, None),
          List()
        ),
        (
          FileListing(new Path(testingBaseDirPath, "src/sub2").toString, None),
          List()
        ),
        (
          FileListing(
            new Path(testingBaseDirPath, "src/sub2/subsub1").toString,
            None
          ),
          List(
            FileListing(new Path(testingBaseDirPath, "src/sub2").toString, None)
          )
        ),
        (
          FileListing(
            new Path(testingBaseDirPath, "src/sub1/2.file").toString,
            Some(15)
          ),
          List(
            FileListing(new Path(testingBaseDirPath, "src/sub1").toString, None)
          )
        ),
        (
          FileListing(
            new Path(testingBaseDirPath, "src/sub2/2.file").toString,
            Some(15)
          ),
          List(
            FileListing(new Path(testingBaseDirPath, "src/sub2").toString, None)
          )
        )
      )
    }

    it("Should list all files in folder with a folder filter") {

      val input = List(
        "src/1.file",
        "src/2.file",
        "src/3.file",
        "src/sub1/1.file",
        "src/sub1/2.file",
        "src/sub1/3.file",
        "src/sub2/1.file",
        "src/sub2/2.file",
        "src/sub2/3.file",
        "src/sub2/subsub1/1.file"
      )

      input.foreach(f => createFile(new Path(f), f.getBytes))

      listFiles(
        localFileSystem,
        new Path(testingBaseDirPath, "src"),
        10,
        false,
        List(""".*/subsub1($|/.*)""".r),
        FailMissingDirectoryAction
      )
        .map(f =>
          (fileStatusToResult(f._1), f._2.map(fileStatusToResult))
        ) should contain theSameElementsAs Seq(
        (
          FileListing(
            new Path(testingBaseDirPath, "src/1.file").toString,
            Some(10)
          ),
          List()
        ),
        (
          FileListing(
            new Path(testingBaseDirPath, "src/2.file").toString,
            Some(10)
          ),
          List()
        ),
        (
          FileListing(
            new Path(testingBaseDirPath, "src/3.file").toString,
            Some(10)
          ),
          List()
        ),
        (
          FileListing(new Path(testingBaseDirPath, "src/sub1").toString, None),
          List()
        ),
        (
          FileListing(new Path(testingBaseDirPath, "src/sub2").toString, None),
          List()
        ),
        (
          FileListing(
            new Path(testingBaseDirPath, "src/sub1/1.file").toString,
            Some(15)
          ),
          List(
            FileListing(new Path(testingBaseDirPath, "src/sub1").toString, None)
          )
        ),
        (
          FileListing(
            new Path(testingBaseDirPath, "src/sub1/2.file").toString,
            Some(15)
          ),
          List(
            FileListing(new Path(testingBaseDirPath, "src/sub1").toString, None)
          )
        ),
        (
          FileListing(
            new Path(testingBaseDirPath, "src/sub1/3.file").toString,
            Some(15)
          ),
          List(
            FileListing(new Path(testingBaseDirPath, "src/sub1").toString, None)
          )
        ),
        (
          FileListing(
            new Path(testingBaseDirPath, "src/sub2/1.file").toString,
            Some(15)
          ),
          List(
            FileListing(new Path(testingBaseDirPath, "src/sub2").toString, None)
          )
        ),
        (
          FileListing(
            new Path(testingBaseDirPath, "src/sub2/2.file").toString,
            Some(15)
          ),
          List(
            FileListing(new Path(testingBaseDirPath, "src/sub2").toString, None)
          )
        ),
        (
          FileListing(
            new Path(testingBaseDirPath, "src/sub2/3.file").toString,
            Some(15)
          ),
          List(
            FileListing(new Path(testingBaseDirPath, "src/sub2").toString, None)
          )
        )
      )
    }

  }

  describe("getSourceFiles") {

    it("throw an exception on source collisions") {

      val spark = new SparkContext(
        new SparkConf().setAppName("test").setMaster("local[*]")
      )

      val input = List("source/first/file", "source/second/file")

      input.foreach(f => createFile(new Path(f), f.getBytes))

      intercept[RuntimeException] {
        getSourceFiles(
          spark,
          Seq(
            new Path(testingBaseDirPath, "source/first").toUri,
            new Path(testingBaseDirPath, "source/second").toUri
          ),
          new Path(testingBaseDirPath, "target").toUri,
          true,
          2,
          List.empty,
          FailMissingDirectoryAction
        )
      }.getMessage should be(
        "Collisions found where multiple source files lead to the same destination location; check executor logs for specific collision detail."
      )

      spark.stop()

    }

    it("throw an exception if source collides with dest") {

      val spark = new SparkContext(
        new SparkConf().setAppName("test").setMaster("local[*]")
      )

      val input = List("source/file")

      input.foreach(f => createFile(new Path(f), f.getBytes))

      intercept[RuntimeException] {
        getSourceFiles(
          spark,
          Seq(new Path(testingBaseDirPath, "source").toUri),
          new Path(testingBaseDirPath, "source").toUri,
          true,
          2,
          List.empty,
          FailMissingDirectoryAction
        )
      }.getMessage should be(
        "Collisions found where a file has the same source and destination location; check executor logs for specific collision detail."
      )

      spark.stop()

    }

  }

}

case class FileListing(name: String, length: Option[Long])
