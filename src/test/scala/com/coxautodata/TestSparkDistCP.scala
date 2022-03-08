package com.coxautodata

import com.coxautodata.SparkDistCP._
import com.coxautodata.objects.{
  CopyDefinitionWithDependencies,
  CopyPartitioner,
  Directory,
  File,
  SerializableFileStatus
}
import com.coxautodata.utils.FileListUtils.listFiles
import com.coxautodata.utils.FileListing
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

class TestSparkDistCP extends TestSpec {

  describe("generateBatchedFileKeys") {

    it("batch files correctly") {

      val in = List(
        CopyDefinitionWithDependencies(
          SerializableFileStatus(new Path("/one").toUri, 0, Directory),
          new Path("/dest/one").toUri,
          Seq.empty
        ),
        CopyDefinitionWithDependencies(
          SerializableFileStatus(new Path("/two").toUri, 0, Directory),
          new Path("/dest/two").toUri,
          Seq.empty
        ),
        CopyDefinitionWithDependencies(
          SerializableFileStatus(new Path("/three").toUri, 0, Directory),
          new Path("/dest/three").toUri,
          Seq.empty
        ),
        CopyDefinitionWithDependencies(
          SerializableFileStatus(new Path("/file1").toUri, 1500, File),
          new Path("/dest/file1").toUri,
          Seq.empty
        ),
        CopyDefinitionWithDependencies(
          SerializableFileStatus(new Path("/file2").toUri, 20, File),
          new Path("/dest/file2").toUri,
          Seq.empty
        ),
        CopyDefinitionWithDependencies(
          SerializableFileStatus(new Path("/file3").toUri, 500, File),
          new Path("/dest/file3").toUri,
          Seq.empty
        )
      )

      generateBatchedFileKeys(3, 2000)(1, in.iterator).toSeq
        .map { case (k, c) =>
          (k, c.source.getPath.toString)
        } should contain theSameElementsInOrderAs Seq(
        ((1, 0), "/one"),
        ((1, 0), "/two"),
        ((1, 0), "/three"),
        ((1, 1), "/file1"),
        ((1, 1), "/file2"),
        ((1, 2), "/file3")
      )

    }

  }

  describe("batchAndPartitionFiles") {

    it("correctly partition files with 2 input partitions") {
      val spark = new SparkContext(
        new SparkConf().setAppName("test").setMaster("local[1]")
      )

      val in = List(
        CopyDefinitionWithDependencies(
          SerializableFileStatus(new Path("/one").toUri, 0, Directory),
          new Path("/dest/one").toUri,
          Seq.empty
        ),
        CopyDefinitionWithDependencies(
          SerializableFileStatus(new Path("/two").toUri, 0, Directory),
          new Path("/dest/two").toUri,
          Seq.empty
        ),
        CopyDefinitionWithDependencies(
          SerializableFileStatus(new Path("/three").toUri, 0, Directory),
          new Path("/dest/three").toUri,
          Seq.empty
        ),
        CopyDefinitionWithDependencies(
          SerializableFileStatus(new Path("/file1").toUri, 1500, File),
          new Path("/dest/file1").toUri,
          Seq.empty
        ),
        CopyDefinitionWithDependencies(
          SerializableFileStatus(new Path("/file2").toUri, 20, File),
          new Path("/dest/file2").toUri,
          Seq.empty
        ),
        CopyDefinitionWithDependencies(
          SerializableFileStatus(new Path("/file3").toUri, 500, File),
          new Path("/dest/file3").toUri,
          Seq.empty
        )
      )

      val inRDD = spark
        .parallelize(in)
        .repartition(2)

      batchAndPartitionFiles(inRDD, 2, 2000)
        .mapPartitionsWithIndex { case (p, v) =>
          List((p, v.toList)).iterator
        }
        .collect()
        .toSeq
        .flatMap { case (p, l) =>
          l.map { case ((pp, i), d) =>
            ((p, pp, i), d.source.getPath.toString)
          }
        } should contain theSameElementsAs Seq(
        ((0, 0, 0), "/file1"),
        ((0, 0, 0), "/file3"),
        ((1, 1, 0), "/file2"),
        ((1, 1, 0), "/one"),
        ((2, 1, 1), "/three"),
        ((2, 1, 1), "/two")
      )

      spark.stop()
    }

    it("correctly partition files with 1 input partition") {
      val spark = new SparkContext(
        new SparkConf().setAppName("test").setMaster("local[1]")
      )

      val in = List(
        CopyDefinitionWithDependencies(
          SerializableFileStatus(new Path("/one").toUri, 0, Directory),
          new Path("/dest/one").toUri,
          Seq.empty
        ),
        CopyDefinitionWithDependencies(
          SerializableFileStatus(new Path("/two").toUri, 0, Directory),
          new Path("/dest/two").toUri,
          Seq.empty
        ),
        CopyDefinitionWithDependencies(
          SerializableFileStatus(new Path("/three").toUri, 0, Directory),
          new Path("/dest/three").toUri,
          Seq.empty
        ),
        CopyDefinitionWithDependencies(
          SerializableFileStatus(new Path("/file1").toUri, 1500, File),
          new Path("/dest/file1").toUri,
          Seq.empty
        ),
        CopyDefinitionWithDependencies(
          SerializableFileStatus(new Path("/file2").toUri, 20, File),
          new Path("/dest/file2").toUri,
          Seq.empty
        ),
        CopyDefinitionWithDependencies(
          SerializableFileStatus(new Path("/file3").toUri, 1990, File),
          new Path("/dest/file3").toUri,
          Seq.empty
        )
      )

      val inRDD = spark
        .parallelize(in)
        .repartition(1)

      batchAndPartitionFiles(inRDD, 2, 2000)
        .mapPartitionsWithIndex { case (p, v) =>
          List((p, v.toList)).iterator
        }
        .collect()
        .toSeq
        .flatMap { case (p, l) =>
          l.map { case ((pp, i), d) =>
            ((p, pp, i), d.source.getPath.toString)
          }
        } should contain theSameElementsAs Seq(
        ((0, 0, 0), "/file1"),
        ((0, 0, 0), "/file2"),
        ((1, 0, 1), "/file3"),
        ((1, 0, 1), "/one"),
        ((2, 0, 2), "/three"),
        ((2, 0, 2), "/two")
      )

      spark.stop()
    }

    it("produce predictable batching") {
      val spark = new SparkContext(
        new SparkConf().setAppName("test").setMaster("local[1]")
      )

      val in = List(
        CopyDefinitionWithDependencies(
          SerializableFileStatus(new Path("/1").toUri, 1, File),
          new Path("/dest/file1").toUri,
          Seq.empty
        ),
        CopyDefinitionWithDependencies(
          SerializableFileStatus(new Path("/3").toUri, 3000, File),
          new Path("/dest/file3").toUri,
          Seq.empty
        ),
        CopyDefinitionWithDependencies(
          SerializableFileStatus(new Path("/2").toUri, 1, File),
          new Path("/dest/file2").toUri,
          Seq.empty
        )
      )

      val inRDD = spark
        .parallelize(in)
        .repartition(1)

      val unsorted = batchAndPartitionFiles(inRDD, 3, 2000).partitioner.get
        .asInstanceOf[CopyPartitioner]

      val sorted = batchAndPartitionFiles(
        inRDD.sortBy(_.source.uri.toString),
        3,
        2000
      ).partitioner.get.asInstanceOf[CopyPartitioner]

      unsorted.indexesAsMap should be(sorted.indexesAsMap)

      spark.stop()
    }

  }

  describe("run") {

    it("perform distcp with non-update/non-overwrite") {

      val spark = SparkSession.builder().master("local[*]").getOrCreate()

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

      val sourceOnlyResult = Seq(
        FileListing(new Path(testingBaseDirPath, "src").toString, None),
        FileListing(new Path(testingBaseDirPath, "dest").toString, None),
        FileListing(
          new Path(testingBaseDirPath, "src/1.file").toString,
          Some(10)
        ),
        FileListing(
          new Path(testingBaseDirPath, "src/2.file").toString,
          Some(10)
        ),
        FileListing(
          new Path(testingBaseDirPath, "src/3.file").toString,
          Some(10)
        ),
        FileListing(new Path(testingBaseDirPath, "src/sub1").toString, None),
        FileListing(new Path(testingBaseDirPath, "src/sub2").toString, None),
        FileListing(
          new Path(testingBaseDirPath, "src/sub2/subsub1").toString,
          None
        ),
        FileListing(
          new Path(testingBaseDirPath, "src/sub1/1.file").toString,
          Some(15)
        ),
        FileListing(
          new Path(testingBaseDirPath, "src/sub1/2.file").toString,
          Some(15)
        ),
        FileListing(
          new Path(testingBaseDirPath, "src/sub1/3.file").toString,
          Some(15)
        ),
        FileListing(
          new Path(testingBaseDirPath, "src/sub2/1.file").toString,
          Some(15)
        ),
        FileListing(
          new Path(testingBaseDirPath, "src/sub2/2.file").toString,
          Some(15)
        ),
        FileListing(
          new Path(testingBaseDirPath, "src/sub2/3.file").toString,
          Some(15)
        ),
        FileListing(
          new Path(testingBaseDirPath, "src/sub2/subsub1/1.file").toString,
          Some(23)
        )
      )

      val destOnlyResult = Seq(
        FileListing(new Path(testingBaseDirPath, "dest/src").toString, None),
        FileListing(
          new Path(testingBaseDirPath, "dest/src/1.file").toString,
          Some(10)
        ),
        FileListing(
          new Path(testingBaseDirPath, "dest/src/2.file").toString,
          Some(10)
        ),
        FileListing(
          new Path(testingBaseDirPath, "dest/src/3.file").toString,
          Some(10)
        ),
        FileListing(
          new Path(testingBaseDirPath, "dest/src/sub1").toString,
          None
        ),
        FileListing(
          new Path(testingBaseDirPath, "dest/src/sub2").toString,
          None
        ),
        FileListing(
          new Path(testingBaseDirPath, "dest/src/sub2/subsub1").toString,
          None
        ),
        FileListing(
          new Path(testingBaseDirPath, "dest/src/sub1/1.file").toString,
          Some(15)
        ),
        FileListing(
          new Path(testingBaseDirPath, "dest/src/sub1/2.file").toString,
          Some(15)
        ),
        FileListing(
          new Path(testingBaseDirPath, "dest/src/sub1/3.file").toString,
          Some(15)
        ),
        FileListing(
          new Path(testingBaseDirPath, "dest/src/sub2/1.file").toString,
          Some(15)
        ),
        FileListing(
          new Path(testingBaseDirPath, "dest/src/sub2/2.file").toString,
          Some(15)
        ),
        FileListing(
          new Path(testingBaseDirPath, "dest/src/sub2/3.file").toString,
          Some(15)
        ),
        FileListing(
          new Path(testingBaseDirPath, "dest/src/sub2/subsub1/1.file").toString,
          Some(23)
        )
      )

      input.foreach(f => createFile(new Path(f), f.getBytes))
      localFileSystem.mkdirs(new Path(testingBaseDirPath, "dest"))

      listFiles(localFileSystem, testingBaseDirPath, 10, false, List.empty, List.empty)
        .map(f =>
          fileStatusToResult(f._1)
        ) should contain theSameElementsAs sourceOnlyResult

      SparkDistCP.run(
        spark,
        Seq(new Path(testingBaseDirPath, "src")),
        new Path(testingBaseDirPath, "dest"),
        SparkDistCPOptions(dryRun = true)
      )

      listFiles(localFileSystem, testingBaseDirPath, 10, false, List.empty, List.empty)
        .map(f =>
          fileStatusToResult(f._1)
        ) should contain theSameElementsAs sourceOnlyResult

      SparkDistCP.run(
        spark,
        Seq(new Path(testingBaseDirPath, "src")),
        new Path(testingBaseDirPath, "dest"),
        SparkDistCPOptions()
      )

      listFiles(localFileSystem, testingBaseDirPath, 10, false, List.empty, List.empty)
        .map(f =>
          fileStatusToResult(f._1)
        ) should contain theSameElementsAs (sourceOnlyResult ++ destOnlyResult)

      spark.stop()

    }

    it("perform distcp with filter") {

      val spark = SparkSession.builder().master("local[*]").getOrCreate()

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

      val sourceOnlyResult = Seq(
        FileListing(new Path(testingBaseDirPath, "src").toString, None),
        FileListing(new Path(testingBaseDirPath, "dest").toString, None),
        FileListing(
          new Path(testingBaseDirPath, "src/1.file").toString,
          Some(10)
        ),
        FileListing(
          new Path(testingBaseDirPath, "src/2.file").toString,
          Some(10)
        ),
        FileListing(
          new Path(testingBaseDirPath, "src/3.file").toString,
          Some(10)
        ),
        FileListing(new Path(testingBaseDirPath, "src/sub1").toString, None),
        FileListing(new Path(testingBaseDirPath, "src/sub2").toString, None),
        FileListing(
          new Path(testingBaseDirPath, "src/sub2/subsub1").toString,
          None
        ),
        FileListing(
          new Path(testingBaseDirPath, "src/sub1/1.file").toString,
          Some(15)
        ),
        FileListing(
          new Path(testingBaseDirPath, "src/sub1/2.file").toString,
          Some(15)
        ),
        FileListing(
          new Path(testingBaseDirPath, "src/sub1/3.file").toString,
          Some(15)
        ),
        FileListing(
          new Path(testingBaseDirPath, "src/sub2/1.file").toString,
          Some(15)
        ),
        FileListing(
          new Path(testingBaseDirPath, "src/sub2/2.file").toString,
          Some(15)
        ),
        FileListing(
          new Path(testingBaseDirPath, "src/sub2/3.file").toString,
          Some(15)
        ),
        FileListing(
          new Path(testingBaseDirPath, "src/sub2/subsub1/1.file").toString,
          Some(23)
        )
      )

      val destOnlyResult = Seq(
        FileListing(new Path(testingBaseDirPath, "dest/src").toString, None),
        FileListing(
          new Path(testingBaseDirPath, "dest/src/2.file").toString,
          Some(10)
        ),
        FileListing(
          new Path(testingBaseDirPath, "dest/src/3.file").toString,
          Some(10)
        ),
        FileListing(
          new Path(testingBaseDirPath, "dest/src/sub1").toString,
          None
        ),
        FileListing(
          new Path(testingBaseDirPath, "dest/src/sub2").toString,
          None
        ),
        FileListing(
          new Path(testingBaseDirPath, "dest/src/sub2/subsub1").toString,
          None
        ),
        FileListing(
          new Path(testingBaseDirPath, "dest/src/sub1/2.file").toString,
          Some(15)
        ),
        FileListing(
          new Path(testingBaseDirPath, "dest/src/sub1/3.file").toString,
          Some(15)
        ),
        FileListing(
          new Path(testingBaseDirPath, "dest/src/sub2/2.file").toString,
          Some(15)
        ),
        FileListing(
          new Path(testingBaseDirPath, "dest/src/sub2/3.file").toString,
          Some(15)
        )
      )

      input.foreach(f => createFile(new Path(f), f.getBytes))
      localFileSystem.mkdirs(new Path(testingBaseDirPath, "dest"))

      listFiles(localFileSystem, testingBaseDirPath, 10, false, List.empty, List.empty)
        .map(f =>
          fileStatusToResult(f._1)
        ) should contain theSameElementsAs sourceOnlyResult

      SparkDistCP.run(
        spark,
        Seq(new Path(testingBaseDirPath, "src")),
        new Path(testingBaseDirPath, "dest"),
        SparkDistCPOptions(dryRun = true, excludesRegex = List(""".*/1\.file$""".r))
      )

      listFiles(localFileSystem, testingBaseDirPath, 10, false, List.empty, List.empty)
        .map(f =>
          fileStatusToResult(f._1)
        ) should contain theSameElementsAs sourceOnlyResult

      SparkDistCP.run(
        spark,
        Seq(new Path(testingBaseDirPath, "src")),
        new Path(testingBaseDirPath, "dest"),
        SparkDistCPOptions(excludesRegex = List(""".*/1\.file$""".r))
      )

      listFiles(localFileSystem, testingBaseDirPath, 10, false, List.empty, List.empty)
        .map(f =>
          fileStatusToResult(f._1)
        ) should contain theSameElementsAs (sourceOnlyResult ++ destOnlyResult)

      spark.stop()

    }

    it("perform distcp with update") {

      val spark = SparkSession.builder().master("local[*]").getOrCreate()

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

      val sourceOnlyResult = Seq(
        FileListing(new Path(testingBaseDirPath, "src").toString, None),
        FileListing(new Path(testingBaseDirPath, "dest").toString, None),
        FileListing(
          new Path(testingBaseDirPath, "src/1.file").toString,
          Some(10)
        ),
        FileListing(
          new Path(testingBaseDirPath, "src/2.file").toString,
          Some(10)
        ),
        FileListing(
          new Path(testingBaseDirPath, "src/3.file").toString,
          Some(10)
        ),
        FileListing(new Path(testingBaseDirPath, "src/sub1").toString, None),
        FileListing(new Path(testingBaseDirPath, "src/sub2").toString, None),
        FileListing(
          new Path(testingBaseDirPath, "src/sub2/subsub1").toString,
          None
        ),
        FileListing(
          new Path(testingBaseDirPath, "src/sub1/1.file").toString,
          Some(15)
        ),
        FileListing(
          new Path(testingBaseDirPath, "src/sub1/2.file").toString,
          Some(15)
        ),
        FileListing(
          new Path(testingBaseDirPath, "src/sub1/3.file").toString,
          Some(15)
        ),
        FileListing(
          new Path(testingBaseDirPath, "src/sub2/1.file").toString,
          Some(15)
        ),
        FileListing(
          new Path(testingBaseDirPath, "src/sub2/2.file").toString,
          Some(15)
        ),
        FileListing(
          new Path(testingBaseDirPath, "src/sub2/3.file").toString,
          Some(15)
        ),
        FileListing(
          new Path(testingBaseDirPath, "src/sub2/subsub1/1.file").toString,
          Some(23)
        )
      )

      val destOnlyResult = Seq(
        FileListing(
          new Path(testingBaseDirPath, "dest/1.file").toString,
          Some(10)
        ),
        FileListing(
          new Path(testingBaseDirPath, "dest/2.file").toString,
          Some(10)
        ),
        FileListing(
          new Path(testingBaseDirPath, "dest/3.file").toString,
          Some(10)
        ),
        FileListing(new Path(testingBaseDirPath, "dest/sub1").toString, None),
        FileListing(new Path(testingBaseDirPath, "dest/sub2").toString, None),
        FileListing(
          new Path(testingBaseDirPath, "dest/sub2/subsub1").toString,
          None
        ),
        FileListing(
          new Path(testingBaseDirPath, "dest/sub1/1.file").toString,
          Some(15)
        ),
        FileListing(
          new Path(testingBaseDirPath, "dest/sub1/2.file").toString,
          Some(15)
        ),
        FileListing(
          new Path(testingBaseDirPath, "dest/sub1/3.file").toString,
          Some(15)
        ),
        FileListing(
          new Path(testingBaseDirPath, "dest/sub2/1.file").toString,
          Some(15)
        ),
        FileListing(
          new Path(testingBaseDirPath, "dest/sub2/2.file").toString,
          Some(15)
        ),
        FileListing(
          new Path(testingBaseDirPath, "dest/sub2/3.file").toString,
          Some(15)
        ),
        FileListing(
          new Path(testingBaseDirPath, "dest/sub2/subsub1/1.file").toString,
          Some(23)
        )
      )

      input.foreach(f => createFile(new Path(f), f.getBytes))
      localFileSystem.mkdirs(new Path(testingBaseDirPath, "dest"))

      listFiles(localFileSystem, testingBaseDirPath, 10, false, List.empty, List.empty)
        .map(f =>
          fileStatusToResult(f._1)
        ) should contain theSameElementsAs sourceOnlyResult

      SparkDistCP.run(
        spark,
        Seq(new Path(testingBaseDirPath, "src")),
        new Path(testingBaseDirPath, "dest"),
        SparkDistCPOptions(dryRun = true, update = true)
      )

      listFiles(localFileSystem, testingBaseDirPath, 10, false, List.empty, List.empty)
        .map(f =>
          fileStatusToResult(f._1)
        ) should contain theSameElementsAs sourceOnlyResult

      SparkDistCP.run(
        spark,
        Seq(new Path(testingBaseDirPath, "src")),
        new Path(testingBaseDirPath, "dest"),
        SparkDistCPOptions(update = true)
      )

      listFiles(localFileSystem, testingBaseDirPath, 10, false, List.empty, List.empty)
        .map(f =>
          fileStatusToResult(f._1)
        ) should contain theSameElementsAs (sourceOnlyResult ++ destOnlyResult)

      spark.stop()

    }

    it(
      "perform distcp with update/overwrite with non-update paths and delete"
    ) {

      val spark = SparkSession.builder().master("local[*]").getOrCreate()

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
        "src/sub2/subsub1/1.file",
        "dest/a.file",
        "dest/suba/b.file",
        "dest/suba/c.file",
        "dest/subb/c.file"
      )

      val sourceOnlyResult = Seq(
        FileListing(new Path(testingBaseDirPath, "src").toString, None),
        FileListing(new Path(testingBaseDirPath, "dest").toString, None),
        FileListing(
          new Path(testingBaseDirPath, "src/1.file").toString,
          Some(10)
        ),
        FileListing(
          new Path(testingBaseDirPath, "src/2.file").toString,
          Some(10)
        ),
        FileListing(
          new Path(testingBaseDirPath, "src/3.file").toString,
          Some(10)
        ),
        FileListing(new Path(testingBaseDirPath, "src/sub1").toString, None),
        FileListing(new Path(testingBaseDirPath, "src/sub2").toString, None),
        FileListing(
          new Path(testingBaseDirPath, "src/sub2/subsub1").toString,
          None
        ),
        FileListing(
          new Path(testingBaseDirPath, "src/sub1/1.file").toString,
          Some(15)
        ),
        FileListing(
          new Path(testingBaseDirPath, "src/sub1/2.file").toString,
          Some(15)
        ),
        FileListing(
          new Path(testingBaseDirPath, "src/sub1/3.file").toString,
          Some(15)
        ),
        FileListing(
          new Path(testingBaseDirPath, "src/sub2/1.file").toString,
          Some(15)
        ),
        FileListing(
          new Path(testingBaseDirPath, "src/sub2/2.file").toString,
          Some(15)
        ),
        FileListing(
          new Path(testingBaseDirPath, "src/sub2/3.file").toString,
          Some(15)
        ),
        FileListing(
          new Path(testingBaseDirPath, "src/sub2/subsub1/1.file").toString,
          Some(23)
        )
      )

      val destExisting = Seq(
        FileListing(
          new Path(testingBaseDirPath, "dest/a.file").toString,
          Some(11)
        ),
        FileListing(new Path(testingBaseDirPath, "dest/suba").toString, None),
        FileListing(new Path(testingBaseDirPath, "dest/subb").toString, None),
        FileListing(
          new Path(testingBaseDirPath, "dest/suba/b.file").toString,
          Some(16)
        ),
        FileListing(
          new Path(testingBaseDirPath, "dest/suba/c.file").toString,
          Some(16)
        ),
        FileListing(
          new Path(testingBaseDirPath, "dest/subb/c.file").toString,
          Some(16)
        )
      )

      val destOnlyResult = Seq(
        FileListing(new Path(testingBaseDirPath, "dest/src").toString, None),
        FileListing(
          new Path(testingBaseDirPath, "dest/src/1.file").toString,
          Some(10)
        ),
        FileListing(
          new Path(testingBaseDirPath, "dest/src/2.file").toString,
          Some(10)
        ),
        FileListing(
          new Path(testingBaseDirPath, "dest/src/3.file").toString,
          Some(10)
        ),
        FileListing(
          new Path(testingBaseDirPath, "dest/src/sub1").toString,
          None
        ),
        FileListing(
          new Path(testingBaseDirPath, "dest/src/sub2").toString,
          None
        ),
        FileListing(
          new Path(testingBaseDirPath, "dest/src/sub2/subsub1").toString,
          None
        ),
        FileListing(
          new Path(testingBaseDirPath, "dest/src/sub1/1.file").toString,
          Some(15)
        ),
        FileListing(
          new Path(testingBaseDirPath, "dest/src/sub1/2.file").toString,
          Some(15)
        ),
        FileListing(
          new Path(testingBaseDirPath, "dest/src/sub1/3.file").toString,
          Some(15)
        ),
        FileListing(
          new Path(testingBaseDirPath, "dest/src/sub2/1.file").toString,
          Some(15)
        ),
        FileListing(
          new Path(testingBaseDirPath, "dest/src/sub2/2.file").toString,
          Some(15)
        ),
        FileListing(
          new Path(testingBaseDirPath, "dest/src/sub2/3.file").toString,
          Some(15)
        ),
        FileListing(
          new Path(testingBaseDirPath, "dest/src/sub2/subsub1/1.file").toString,
          Some(23)
        )
      )

      input.foreach(f => createFile(new Path(f), f.getBytes))

      listFiles(localFileSystem, testingBaseDirPath, 10, false, List.empty, List.empty)
        .map(f =>
          fileStatusToResult(f._1)
        ) should contain theSameElementsAs (sourceOnlyResult ++ destExisting)

      SparkDistCP.run(
        spark,
        Seq(new Path(testingBaseDirPath, "src")),
        new Path(testingBaseDirPath, "dest"),
        SparkDistCPOptions(
          dryRun = true,
          consistentPathBehaviour = true,
          overwrite = true,
          delete = true
        )
      )

      listFiles(localFileSystem, testingBaseDirPath, 10, false, List.empty, List.empty)
        .map(f =>
          fileStatusToResult(f._1)
        ) should contain theSameElementsAs (sourceOnlyResult ++ destExisting)

      SparkDistCP.run(
        spark,
        Seq(new Path(testingBaseDirPath, "src")),
        new Path(testingBaseDirPath, "dest"),
        SparkDistCPOptions(
          consistentPathBehaviour = true,
          overwrite = true,
          delete = true
        )
      )

      listFiles(localFileSystem, testingBaseDirPath, 10, false, List.empty, List.empty)
        .map(f =>
          fileStatusToResult(f._1)
        ) should contain theSameElementsAs (sourceOnlyResult ++ destOnlyResult)

      spark.stop()

    }

    it("perform distcp with update/overwrite with delete") {

      val spark = SparkSession.builder().master("local[*]").getOrCreate()

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
        "src/sub2/subsub1/1.file",
        "dest/a.file",
        "dest/suba/b.file",
        "dest/suba/c.file",
        "dest/subb/c.file"
      )

      val sourceOnlyResult = Seq(
        FileListing(new Path(testingBaseDirPath, "src").toString, None),
        FileListing(new Path(testingBaseDirPath, "dest").toString, None),
        FileListing(
          new Path(testingBaseDirPath, "src/1.file").toString,
          Some(10)
        ),
        FileListing(
          new Path(testingBaseDirPath, "src/2.file").toString,
          Some(10)
        ),
        FileListing(
          new Path(testingBaseDirPath, "src/3.file").toString,
          Some(10)
        ),
        FileListing(new Path(testingBaseDirPath, "src/sub1").toString, None),
        FileListing(new Path(testingBaseDirPath, "src/sub2").toString, None),
        FileListing(
          new Path(testingBaseDirPath, "src/sub2/subsub1").toString,
          None
        ),
        FileListing(
          new Path(testingBaseDirPath, "src/sub1/1.file").toString,
          Some(15)
        ),
        FileListing(
          new Path(testingBaseDirPath, "src/sub1/2.file").toString,
          Some(15)
        ),
        FileListing(
          new Path(testingBaseDirPath, "src/sub1/3.file").toString,
          Some(15)
        ),
        FileListing(
          new Path(testingBaseDirPath, "src/sub2/1.file").toString,
          Some(15)
        ),
        FileListing(
          new Path(testingBaseDirPath, "src/sub2/2.file").toString,
          Some(15)
        ),
        FileListing(
          new Path(testingBaseDirPath, "src/sub2/3.file").toString,
          Some(15)
        ),
        FileListing(
          new Path(testingBaseDirPath, "src/sub2/subsub1/1.file").toString,
          Some(23)
        )
      )

      val destExisting = Seq(
        FileListing(
          new Path(testingBaseDirPath, "dest/a.file").toString,
          Some(11)
        ),
        FileListing(new Path(testingBaseDirPath, "dest/suba").toString, None),
        FileListing(new Path(testingBaseDirPath, "dest/subb").toString, None),
        FileListing(
          new Path(testingBaseDirPath, "dest/suba/b.file").toString,
          Some(16)
        ),
        FileListing(
          new Path(testingBaseDirPath, "dest/suba/c.file").toString,
          Some(16)
        ),
        FileListing(
          new Path(testingBaseDirPath, "dest/subb/c.file").toString,
          Some(16)
        )
      )

      val destOnlyResult = Seq(
        FileListing(
          new Path(testingBaseDirPath, "dest/1.file").toString,
          Some(10)
        ),
        FileListing(
          new Path(testingBaseDirPath, "dest/2.file").toString,
          Some(10)
        ),
        FileListing(
          new Path(testingBaseDirPath, "dest/3.file").toString,
          Some(10)
        ),
        FileListing(new Path(testingBaseDirPath, "dest/sub1").toString, None),
        FileListing(new Path(testingBaseDirPath, "dest/sub2").toString, None),
        FileListing(
          new Path(testingBaseDirPath, "dest/sub2/subsub1").toString,
          None
        ),
        FileListing(
          new Path(testingBaseDirPath, "dest/sub1/1.file").toString,
          Some(15)
        ),
        FileListing(
          new Path(testingBaseDirPath, "dest/sub1/2.file").toString,
          Some(15)
        ),
        FileListing(
          new Path(testingBaseDirPath, "dest/sub1/3.file").toString,
          Some(15)
        ),
        FileListing(
          new Path(testingBaseDirPath, "dest/sub2/1.file").toString,
          Some(15)
        ),
        FileListing(
          new Path(testingBaseDirPath, "dest/sub2/2.file").toString,
          Some(15)
        ),
        FileListing(
          new Path(testingBaseDirPath, "dest/sub2/3.file").toString,
          Some(15)
        ),
        FileListing(
          new Path(testingBaseDirPath, "dest/sub2/subsub1/1.file").toString,
          Some(23)
        )
      )

      input.foreach(f => createFile(new Path(f), f.getBytes))

      listFiles(localFileSystem, testingBaseDirPath, 10, false, List.empty, List.empty)
        .map(f =>
          fileStatusToResult(f._1)
        ) should contain theSameElementsAs (sourceOnlyResult ++ destExisting)

      SparkDistCP.run(
        spark,
        Seq(new Path(testingBaseDirPath, "src")),
        new Path(testingBaseDirPath, "dest"),
        SparkDistCPOptions(dryRun = true, overwrite = true, delete = true)
      )

      listFiles(localFileSystem, testingBaseDirPath, 10, false, List.empty, List.empty)
        .map(f =>
          fileStatusToResult(f._1)
        ) should contain theSameElementsAs (sourceOnlyResult ++ destExisting)

      SparkDistCP.run(
        spark,
        Seq(new Path(testingBaseDirPath, "src")),
        new Path(testingBaseDirPath, "dest"),
        SparkDistCPOptions(overwrite = true, delete = true)
      )

      listFiles(localFileSystem, testingBaseDirPath, 10, false, List.empty, List.empty)
        .map(f =>
          fileStatusToResult(f._1)
        ) should contain theSameElementsAs (sourceOnlyResult ++ destOnlyResult)

      spark.stop()

    }

    it(
      "perform distcp with update/overwrite with non-update paths and delete and filter"
    ) {

      val spark = SparkSession.builder().master("local[*]").getOrCreate()

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
        "src/sub2/subsub1/1.file",
        "dest/a.file",
        "dest/c.file",
        "dest/suba/b.file",
        "dest/suba/c.file",
        "dest/subb/c.file"
      )

      val sourceOnlyResult = Seq(
        FileListing(new Path(testingBaseDirPath, "src").toString, None),
        FileListing(new Path(testingBaseDirPath, "dest").toString, None),
        FileListing(
          new Path(testingBaseDirPath, "src/1.file").toString,
          Some(10)
        ),
        FileListing(
          new Path(testingBaseDirPath, "src/2.file").toString,
          Some(10)
        ),
        FileListing(
          new Path(testingBaseDirPath, "src/3.file").toString,
          Some(10)
        ),
        FileListing(new Path(testingBaseDirPath, "src/sub1").toString, None),
        FileListing(new Path(testingBaseDirPath, "src/sub2").toString, None),
        FileListing(
          new Path(testingBaseDirPath, "src/sub2/subsub1").toString,
          None
        ),
        FileListing(
          new Path(testingBaseDirPath, "src/sub1/1.file").toString,
          Some(15)
        ),
        FileListing(
          new Path(testingBaseDirPath, "src/sub1/2.file").toString,
          Some(15)
        ),
        FileListing(
          new Path(testingBaseDirPath, "src/sub1/3.file").toString,
          Some(15)
        ),
        FileListing(
          new Path(testingBaseDirPath, "src/sub2/1.file").toString,
          Some(15)
        ),
        FileListing(
          new Path(testingBaseDirPath, "src/sub2/2.file").toString,
          Some(15)
        ),
        FileListing(
          new Path(testingBaseDirPath, "src/sub2/3.file").toString,
          Some(15)
        ),
        FileListing(
          new Path(testingBaseDirPath, "src/sub2/subsub1/1.file").toString,
          Some(23)
        )
      )

      val destExisting = Seq(
        FileListing(
          new Path(testingBaseDirPath, "dest/a.file").toString,
          Some(11)
        ),
        FileListing(
          new Path(testingBaseDirPath, "dest/c.file").toString,
          Some(11)
        ),
        FileListing(new Path(testingBaseDirPath, "dest/suba").toString, None),
        FileListing(new Path(testingBaseDirPath, "dest/subb").toString, None),
        FileListing(
          new Path(testingBaseDirPath, "dest/suba/b.file").toString,
          Some(16)
        ),
        FileListing(
          new Path(testingBaseDirPath, "dest/suba/c.file").toString,
          Some(16)
        ),
        FileListing(
          new Path(testingBaseDirPath, "dest/subb/c.file").toString,
          Some(16)
        )
      )

      val destOnlyResult = Seq(
        FileListing(new Path(testingBaseDirPath, "dest/src").toString, None),
        FileListing(
          new Path(testingBaseDirPath, "dest/src/2.file").toString,
          Some(10)
        ),
        FileListing(
          new Path(testingBaseDirPath, "dest/src/3.file").toString,
          Some(10)
        ),
        FileListing(
          new Path(testingBaseDirPath, "dest/src/sub1").toString,
          None
        ),
        FileListing(
          new Path(testingBaseDirPath, "dest/src/sub2").toString,
          None
        ),
        FileListing(
          new Path(testingBaseDirPath, "dest/src/sub2/subsub1").toString,
          None
        ),
        FileListing(
          new Path(testingBaseDirPath, "dest/src/sub1/2.file").toString,
          Some(15)
        ),
        FileListing(
          new Path(testingBaseDirPath, "dest/src/sub1/3.file").toString,
          Some(15)
        ),
        FileListing(
          new Path(testingBaseDirPath, "dest/src/sub2/2.file").toString,
          Some(15)
        ),
        FileListing(
          new Path(testingBaseDirPath, "dest/src/sub2/3.file").toString,
          Some(15)
        )
      )

      input.foreach(f => createFile(new Path(f), f.getBytes))

      listFiles(localFileSystem, testingBaseDirPath, 10, false, List.empty, List.empty)
        .map(f =>
          fileStatusToResult(f._1)
        ) should contain theSameElementsAs (sourceOnlyResult ++ destExisting)

      SparkDistCP.run(
        spark,
        Seq(new Path(testingBaseDirPath, "src")),
        new Path(testingBaseDirPath, "dest"),
        SparkDistCPOptions(
          dryRun = true,
          consistentPathBehaviour = true,
          overwrite = true,
          delete = true,
          excludesRegex = List(""".*/1\.file$""".r, """.*/c\.file$""".r)
        )
      )

      listFiles(localFileSystem, testingBaseDirPath, 10, false, List.empty, List.empty)
        .map(f =>
          fileStatusToResult(f._1)
        ) should contain theSameElementsAs (sourceOnlyResult ++ destExisting)

      SparkDistCP.run(
        spark,
        Seq(new Path(testingBaseDirPath, "src")),
        new Path(testingBaseDirPath, "dest"),
        SparkDistCPOptions(
          consistentPathBehaviour = true,
          overwrite = true,
          delete = true,
          excludesRegex = List(""".*/1\.file$""".r, """.*/c\.file$""".r)
        )
      )

      listFiles(localFileSystem, testingBaseDirPath, 10, false, List.empty, List.empty)
        .map(f =>
          fileStatusToResult(f._1)
        ) should contain theSameElementsAs (sourceOnlyResult ++ destOnlyResult)

      spark.stop()

    }

    it("perform distcp with update/overwrite with and delete and filter") {

      val spark = SparkSession.builder().master("local[*]").getOrCreate()

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
        "src/sub2/subsub1/1.file",
        "dest/a.file",
        "dest/c.file",
        "dest/suba/b.file",
        "dest/suba/c.file",
        "dest/subb/c.file"
      )

      val sourceOnlyResult = Seq(
        FileListing(new Path(testingBaseDirPath, "src").toString, None),
        FileListing(new Path(testingBaseDirPath, "dest").toString, None),
        FileListing(
          new Path(testingBaseDirPath, "src/1.file").toString,
          Some(10)
        ),
        FileListing(
          new Path(testingBaseDirPath, "src/2.file").toString,
          Some(10)
        ),
        FileListing(
          new Path(testingBaseDirPath, "src/3.file").toString,
          Some(10)
        ),
        FileListing(new Path(testingBaseDirPath, "src/sub1").toString, None),
        FileListing(new Path(testingBaseDirPath, "src/sub2").toString, None),
        FileListing(
          new Path(testingBaseDirPath, "src/sub2/subsub1").toString,
          None
        ),
        FileListing(
          new Path(testingBaseDirPath, "src/sub1/1.file").toString,
          Some(15)
        ),
        FileListing(
          new Path(testingBaseDirPath, "src/sub1/2.file").toString,
          Some(15)
        ),
        FileListing(
          new Path(testingBaseDirPath, "src/sub1/3.file").toString,
          Some(15)
        ),
        FileListing(
          new Path(testingBaseDirPath, "src/sub2/1.file").toString,
          Some(15)
        ),
        FileListing(
          new Path(testingBaseDirPath, "src/sub2/2.file").toString,
          Some(15)
        ),
        FileListing(
          new Path(testingBaseDirPath, "src/sub2/3.file").toString,
          Some(15)
        ),
        FileListing(
          new Path(testingBaseDirPath, "src/sub2/subsub1/1.file").toString,
          Some(23)
        )
      )

      val destExisting = Seq(
        FileListing(
          new Path(testingBaseDirPath, "dest/a.file").toString,
          Some(11)
        ),
        FileListing(
          new Path(testingBaseDirPath, "dest/c.file").toString,
          Some(11)
        ),
        FileListing(new Path(testingBaseDirPath, "dest/suba").toString, None),
        FileListing(new Path(testingBaseDirPath, "dest/subb").toString, None),
        FileListing(
          new Path(testingBaseDirPath, "dest/suba/b.file").toString,
          Some(16)
        ),
        FileListing(
          new Path(testingBaseDirPath, "dest/suba/c.file").toString,
          Some(16)
        ),
        FileListing(
          new Path(testingBaseDirPath, "dest/subb/c.file").toString,
          Some(16)
        )
      )

      val destOnlyResult = Seq(
        FileListing(
          new Path(testingBaseDirPath, "dest/2.file").toString,
          Some(10)
        ),
        FileListing(
          new Path(testingBaseDirPath, "dest/3.file").toString,
          Some(10)
        ),
        FileListing(new Path(testingBaseDirPath, "dest/sub1").toString, None),
        FileListing(new Path(testingBaseDirPath, "dest/sub2").toString, None),
        FileListing(
          new Path(testingBaseDirPath, "dest/sub2/subsub1").toString,
          None
        ),
        FileListing(
          new Path(testingBaseDirPath, "dest/sub1/2.file").toString,
          Some(15)
        ),
        FileListing(
          new Path(testingBaseDirPath, "dest/sub1/3.file").toString,
          Some(15)
        ),
        FileListing(
          new Path(testingBaseDirPath, "dest/sub2/2.file").toString,
          Some(15)
        ),
        FileListing(
          new Path(testingBaseDirPath, "dest/sub2/3.file").toString,
          Some(15)
        )
      )

      input.foreach(f => createFile(new Path(f), f.getBytes))

      listFiles(localFileSystem, testingBaseDirPath, 10, false, List.empty, List.empty)
        .map(f =>
          fileStatusToResult(f._1)
        ) should contain theSameElementsAs (sourceOnlyResult ++ destExisting)

      SparkDistCP.run(
        spark,
        Seq(new Path(testingBaseDirPath, "src")),
        new Path(testingBaseDirPath, "dest"),
        SparkDistCPOptions(
          dryRun = true,
          overwrite = true,
          delete = true,
          excludesRegex = List(""".*/1\.file$""".r, """.*/c\.file$""".r)
        )
      )

      listFiles(localFileSystem, testingBaseDirPath, 10, false, List.empty, List.empty)
        .map(f =>
          fileStatusToResult(f._1)
        ) should contain theSameElementsAs (sourceOnlyResult ++ destExisting)

      SparkDistCP.run(
        spark,
        Seq(new Path(testingBaseDirPath, "src")),
        new Path(testingBaseDirPath, "dest"),
        SparkDistCPOptions(
          overwrite = true,
          delete = true,
          excludesRegex = List(""".*/1\.file$""".r, """.*/c\.file$""".r)
        )
      )

      listFiles(localFileSystem, testingBaseDirPath, 10, false, List.empty, List.empty)
        .map(f =>
          fileStatusToResult(f._1)
        ) should contain theSameElementsAs (sourceOnlyResult ++ destOnlyResult)

      spark.stop()

    }

    it("perform distcp with non-update/non-overwrite and logging") {

      val spark = SparkSession.builder().master("local[*]").getOrCreate()

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
      localFileSystem.mkdirs(new Path(testingBaseDirPath, "dest"))

      localFileSystem.exists(
        new Path(testingBaseDirPath, "dryrun.log")
      ) should be(false)

      SparkDistCP.run(
        spark,
        Seq(new Path(testingBaseDirPath, "src")),
        new Path(testingBaseDirPath, "dest"),
        SparkDistCPOptions(
          dryRun = true,
          log = Some(new Path(testingBaseDirPath, "dryrun.log").toUri)
        )
      )

      localFileSystem.exists(
        new Path(testingBaseDirPath, "dryrun.log")
      ) should be(true)

      localFileSystem.exists(new Path(testingBaseDirPath, "run.log")) should be(
        false
      )

      SparkDistCP.run(
        spark,
        Seq(new Path(testingBaseDirPath, "src")),
        new Path(testingBaseDirPath, "dest"),
        SparkDistCPOptions(log =
          Some(new Path(testingBaseDirPath, "run.log").toUri)
        )
      )

      localFileSystem.exists(new Path(testingBaseDirPath, "run.log")) should be(
        true
      )

      spark.stop()

    }

    it("provide an empty source path list") {
      val spark = SparkSession.builder().master("local[*]").getOrCreate()

      intercept[java.lang.AssertionError] {
        SparkDistCP.run(
          spark,
          Seq.empty,
          new Path("test"),
          SparkDistCPOptions()
        )
      }.getMessage should be(
        "assertion failed: At least one source path must be given"
      )

      spark.stop()
    }

    it("provide an incorrect options configuration") {
      val spark = SparkSession.builder().master("local[*]").getOrCreate()

      intercept[java.lang.AssertionError] {
        SparkDistCP.run(
          spark,
          Seq(new Path("src")),
          new Path("dest"),
          SparkDistCPOptions(update = true, overwrite = true)
        )
      }.getMessage should be(
        "assertion failed: Both update and overwrite cannot be specified"
      )

      spark.stop()
    }

  }

}
