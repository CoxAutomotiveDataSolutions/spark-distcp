package com.coxautodata

import com.coxautodata.SparkDistCP._
import com.coxautodata.TestSparkDistCP.listFiles
import com.coxautodata.objects.{CopyDefinitionWithDependencies, CopyPartitioner, Directory, File, FileType, SerializableFileStatus}
import com.coxautodata.utils.{FailMissingDirectoryAction, FileListUtils, FileListing}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

class TestSparkDistCP extends TestSpec {
  def copyDefinitionWithDependencies(src: String, dest: String, len: Long, ft: FileType): CopyDefinitionWithDependencies =
    CopyDefinitionWithDependencies(
      SerializableFileStatus(new Path(src).toUri, len, ft),
      new Path(dest).toUri,
      Seq.empty
    )

  def copyDefinitions(): List[CopyDefinitionWithDependencies] = {
    List(
      copyDefinitionWithDependencies("/one", "/dest/one", 0, Directory),
      copyDefinitionWithDependencies("/two", "/dest/two", 0, Directory),
      copyDefinitionWithDependencies("/three", "/dest/three", 0, Directory),
      copyDefinitionWithDependencies("/file1", "/dest/file1", 1500, File),
      copyDefinitionWithDependencies("/file2", "/dest/file2", 20, File),
      copyDefinitionWithDependencies("/file3", "/dest/file3", 500, File)
    )
  }

  def fileListing(name: String, length: Option[Long]): FileListing =
    FileListing(new Path(testingBaseDirPath, name).toString, length)

  def fileListings(): Seq[FileListing] = {
    Seq(
      fileListing("src", None),
      fileListing("dest", None),
      fileListing("src/1.file", Some(10)),
      fileListing("src/2.file", Some(10)),
      fileListing("src/3.file", Some(10)),
      fileListing("src/sub1", None),
      fileListing("src/sub2", None),
      fileListing("src/sub2/subsub1", None),
      fileListing("src/sub1/1.file", Some(15)),
      fileListing("src/sub1/2.file", Some(15)),
      fileListing("src/sub1/3.file", Some(15)),
      fileListing("src/sub2/1.file", Some(15)),
      fileListing("src/sub2/2.file", Some(15)),
      fileListing("src/sub2/3.file", Some(15)),
      fileListing("src/sub2/subsub1/1.file", Some(23))
    )
  }

  def files(): List[String] = fileListings().filter(_.length.nonEmpty)
    .map(_.name.substring(testingBaseDirPath.toString.length + 1)).toList

  describe("generateBatchedFileKeys") {

    it("batch files correctly") {

      val in = copyDefinitions()

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

      val in = copyDefinitions()

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
        copyDefinitionWithDependencies("/one", "/dest/one", 0, Directory),
        copyDefinitionWithDependencies("/two", "/dest/two", 0, Directory),
        copyDefinitionWithDependencies("/three", "/dest/three", 0, Directory),
        copyDefinitionWithDependencies("/file1", "/dest/file1", 1500, File),
        copyDefinitionWithDependencies("/file2", "/dest/file2", 20, File),
        copyDefinitionWithDependencies("/file3", "/dest/file3", 1990, File)
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
        copyDefinitionWithDependencies("/1", "/dest/file1", 1, File),
        copyDefinitionWithDependencies("/3", "/dest/file3", 3000, File),
        copyDefinitionWithDependencies("/2", "/dest/file2", 1, File)
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

      val input = files()

      val sourceOnlyResult = fileListings()

      val destOnlyResult = Seq(
        fileListing("dest/src", None),
        fileListing("dest/src/1.file", Some(10)),
        fileListing("dest/src/2.file", Some(10)),
        fileListing("dest/src/3.file", Some(10)),
        fileListing("dest/src/sub1", None),
        fileListing("dest/src/sub2", None),
        fileListing("dest/src/sub2/subsub1", None),
        fileListing("dest/src/sub1/1.file", Some(15)),
        fileListing("dest/src/sub1/2.file", Some(15)),
        fileListing("dest/src/sub1/3.file", Some(15)),
        fileListing("dest/src/sub2/1.file", Some(15)),
        fileListing("dest/src/sub2/2.file", Some(15)),
        fileListing("dest/src/sub2/3.file", Some(15)),
        fileListing("dest/src/sub2/subsub1/1.file", Some(23))
      )

      input.foreach(f => createFile(new Path(f), f.getBytes))
      localFileSystem.mkdirs(new Path(testingBaseDirPath, "dest"))

      listFiles(localFileSystem, testingBaseDirPath)
        .map(f =>
          fileStatusToResult(f._1)
        ) should contain theSameElementsAs sourceOnlyResult

      SparkDistCP.run(
        spark,
        Seq(new Path(testingBaseDirPath, "src")),
        new Path(testingBaseDirPath, "dest"),
        SparkDistCPOptions(dryRun = true)
      )

      listFiles(localFileSystem, testingBaseDirPath)
        .map(f =>
          fileStatusToResult(f._1)
        ) should contain theSameElementsAs sourceOnlyResult

      SparkDistCP.run(
        spark,
        Seq(new Path(testingBaseDirPath, "src")),
        new Path(testingBaseDirPath, "dest"),
        SparkDistCPOptions()
      )

      listFiles(localFileSystem, testingBaseDirPath)
        .map(f =>
          fileStatusToResult(f._1)
        ) should contain theSameElementsAs (sourceOnlyResult ++ destOnlyResult)

      spark.stop()

    }

    it("perform distcp with filter") {

      val spark = SparkSession.builder().master("local[*]").getOrCreate()

      val input = files()

      val sourceOnlyResult = fileListings()

      val destOnlyResult = Seq(
        fileListing("dest/src", None),
        fileListing("dest/src/2.file", Some(10)),
        fileListing("dest/src/3.file", Some(10)),
        fileListing("dest/src/sub1", None),
        fileListing("dest/src/sub2", None),
        fileListing("dest/src/sub2/subsub1", None),
        fileListing("dest/src/sub1/2.file", Some(15)),
        fileListing("dest/src/sub1/3.file", Some(15)),
        fileListing("dest/src/sub2/2.file", Some(15)),
        fileListing("dest/src/sub2/3.file", Some(15))
      )

      input.foreach(f => createFile(new Path(f), f.getBytes))
      localFileSystem.mkdirs(new Path(testingBaseDirPath, "dest"))

      listFiles(localFileSystem, testingBaseDirPath)
        .map(f =>
          fileStatusToResult(f._1)
        ) should contain theSameElementsAs sourceOnlyResult

      SparkDistCP.run(
        spark,
        Seq(new Path(testingBaseDirPath, "src")),
        new Path(testingBaseDirPath, "dest"),
        SparkDistCPOptions(dryRun = true, filterNot = List(""".*/1\.file$""".r))
      )

      listFiles(localFileSystem, testingBaseDirPath)
        .map(f =>
          fileStatusToResult(f._1)
        ) should contain theSameElementsAs sourceOnlyResult

      SparkDistCP.run(
        spark,
        Seq(new Path(testingBaseDirPath, "src")),
        new Path(testingBaseDirPath, "dest"),
        SparkDistCPOptions(filterNot = List(""".*/1\.file$""".r))
      )

      listFiles(localFileSystem, testingBaseDirPath)
        .map(f =>
          fileStatusToResult(f._1)
        ) should contain theSameElementsAs (sourceOnlyResult ++ destOnlyResult)

      spark.stop()

    }

    it("perform distcp with update") {

      val spark = SparkSession.builder().master("local[*]").getOrCreate()

      val input = files()

      val sourceOnlyResult = fileListings()

      val destOnlyResult = Seq(
        fileListing("dest/1.file", Some(10)),
        fileListing("dest/2.file", Some(10)),
        fileListing("dest/3.file", Some(10)),
        fileListing("dest/sub1", None),
        fileListing("dest/sub2", None),
        fileListing("dest/sub2/subsub1", None),
        fileListing("dest/sub1/1.file", Some(15)),
        fileListing("dest/sub1/2.file", Some(15)),
        fileListing("dest/sub1/3.file", Some(15)),
        fileListing("dest/sub2/1.file", Some(15)),
        fileListing("dest/sub2/2.file", Some(15)),
        fileListing("dest/sub2/3.file", Some(15)),
        fileListing("dest/sub2/subsub1/1.file", Some(23))
      )

      input.foreach(f => createFile(new Path(f), f.getBytes))
      localFileSystem.mkdirs(new Path(testingBaseDirPath, "dest"))

      listFiles(localFileSystem, testingBaseDirPath)
        .map(f =>
          fileStatusToResult(f._1)
        ) should contain theSameElementsAs sourceOnlyResult

      SparkDistCP.run(
        spark,
        Seq(new Path(testingBaseDirPath, "src")),
        new Path(testingBaseDirPath, "dest"),
        SparkDistCPOptions(dryRun = true, update = true)
      )

      listFiles(localFileSystem, testingBaseDirPath)
        .map(f =>
          fileStatusToResult(f._1)
        ) should contain theSameElementsAs sourceOnlyResult

      SparkDistCP.run(
        spark,
        Seq(new Path(testingBaseDirPath, "src")),
        new Path(testingBaseDirPath, "dest"),
        SparkDistCPOptions(update = true)
      )

      listFiles(localFileSystem, testingBaseDirPath)
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

      val sourceOnlyResult = fileListings()

      val destExisting = Seq(
        fileListing("dest/a.file", Some(11)),
        fileListing("dest/suba", None),
        fileListing("dest/subb", None),
        fileListing("dest/suba/b.file", Some(16)),
        fileListing("dest/suba/c.file", Some(16)),
        fileListing("dest/subb/c.file", Some(16))
      )

      val destOnlyResult = Seq(
        fileListing("dest/src", None),
        fileListing("dest/src/1.file", Some(10)),
        fileListing("dest/src/2.file", Some(10)),
        fileListing("dest/src/3.file", Some(10)),
        fileListing("dest/src/sub1", None),
        fileListing("dest/src/sub2", None),
        fileListing("dest/src/sub2/subsub1", None),
        fileListing("dest/src/sub1/1.file", Some(15)),
        fileListing("dest/src/sub1/2.file", Some(15)),
        fileListing("dest/src/sub1/3.file", Some(15)),
        fileListing("dest/src/sub2/1.file", Some(15)),
        fileListing("dest/src/sub2/2.file", Some(15)),
        fileListing("dest/src/sub2/3.file", Some(15)),
        fileListing("dest/src/sub2/subsub1/1.file", Some(23))
      )

      input.foreach(f => createFile(new Path(f), f.getBytes))

      listFiles(localFileSystem, testingBaseDirPath)
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

      listFiles(localFileSystem, testingBaseDirPath)
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

      listFiles(localFileSystem, testingBaseDirPath)
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

      val sourceOnlyResult = fileListings()

      val destExisting = Seq(
        fileListing("dest/a.file", Some(11)),
        fileListing("dest/suba", None),
        fileListing("dest/subb", None),
        fileListing("dest/suba/b.file", Some(16)),
        fileListing("dest/suba/c.file", Some(16)),
        fileListing("dest/subb/c.file", Some(16))
      )

      val destOnlyResult = Seq(
        fileListing("dest/1.file", Some(10)),
        fileListing("dest/2.file", Some(10)),
        fileListing("dest/3.file", Some(10)),
        fileListing("dest/sub1", None),
        fileListing("dest/sub2", None),
        fileListing("dest/sub2/subsub1", None),
        fileListing("dest/sub1/1.file", Some(15)),
        fileListing("dest/sub1/2.file", Some(15)),
        fileListing("dest/sub1/3.file", Some(15)),
        fileListing("dest/sub2/1.file", Some(15)),
        fileListing("dest/sub2/2.file", Some(15)),
        fileListing("dest/sub2/3.file", Some(15)),
        fileListing("dest/sub2/subsub1/1.file", Some(23))
      )

      input.foreach(f => createFile(new Path(f), f.getBytes))

      listFiles(localFileSystem, testingBaseDirPath)
        .map(f =>
          fileStatusToResult(f._1)
        ) should contain theSameElementsAs (sourceOnlyResult ++ destExisting)

      SparkDistCP.run(
        spark,
        Seq(new Path(testingBaseDirPath, "src")),
        new Path(testingBaseDirPath, "dest"),
        SparkDistCPOptions(dryRun = true, overwrite = true, delete = true)
      )

      listFiles(localFileSystem, testingBaseDirPath)
        .map(f =>
          fileStatusToResult(f._1)
        ) should contain theSameElementsAs (sourceOnlyResult ++ destExisting)

      SparkDistCP.run(
        spark,
        Seq(new Path(testingBaseDirPath, "src")),
        new Path(testingBaseDirPath, "dest"),
        SparkDistCPOptions(overwrite = true, delete = true)
      )

      listFiles(localFileSystem, testingBaseDirPath)
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

      val sourceOnlyResult = fileListings()

      val destExisting = Seq(
        fileListing("dest/a.file", Some(11)),
        fileListing("dest/c.file", Some(11)),
        fileListing("dest/suba", None),
        fileListing("dest/subb", None),
        fileListing("dest/suba/b.file", Some(16)),
        fileListing("dest/suba/c.file", Some(16)),
        fileListing("dest/subb/c.file", Some(16))
      )

      val destOnlyResult = Seq(
        fileListing("dest/src", None),
        fileListing("dest/src/2.file", Some(10)),
        fileListing("dest/src/3.file", Some(10)),
        fileListing("dest/src/sub1", None),
        fileListing("dest/src/sub2", None),
        fileListing("dest/src/sub2/subsub1", None),
        fileListing("dest/src/sub1/2.file", Some(15)),
        fileListing("dest/src/sub1/3.file", Some(15)),
        fileListing("dest/src/sub2/2.file", Some(15)),
        fileListing("dest/src/sub2/3.file", Some(15))
      )

      input.foreach(f => createFile(new Path(f), f.getBytes))

      listFiles(localFileSystem, testingBaseDirPath)
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
          filterNot = List(""".*/1\.file$""".r, """.*/c\.file$""".r)
        )
      )

      listFiles(localFileSystem, testingBaseDirPath)
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
          filterNot = List(""".*/1\.file$""".r, """.*/c\.file$""".r)
        )
      )

      listFiles(localFileSystem, testingBaseDirPath)
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

      val sourceOnlyResult = fileListings()

      val destExisting = Seq(
        fileListing("dest/a.file", Some(11)),
        fileListing("dest/c.file", Some(11)),
        fileListing("dest/suba", None),
        fileListing("dest/subb", None),
        fileListing("dest/suba/b.file", Some(16)),
        fileListing("dest/suba/c.file", Some(16)),
        fileListing("dest/subb/c.file", Some(16))
      )

      val destOnlyResult = Seq(
        fileListing("dest/2.file", Some(10)),
        fileListing("dest/3.file", Some(10)),
        fileListing("dest/sub1", None),
        fileListing("dest/sub2", None),
        fileListing("dest/sub2/subsub1", None),
        fileListing("dest/sub1/2.file", Some(15)),
        fileListing("dest/sub1/3.file", Some(15)),
        fileListing("dest/sub2/2.file", Some(15)),
        fileListing("dest/sub2/3.file", Some(15))
      )

      input.foreach(f => createFile(new Path(f), f.getBytes))

      listFiles(localFileSystem, testingBaseDirPath)
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
          filterNot = List(""".*/1\.file$""".r, """.*/c\.file$""".r)
        )
      )

      listFiles(localFileSystem, testingBaseDirPath)
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
          filterNot = List(""".*/1\.file$""".r, """.*/c\.file$""".r)
        )
      )

      listFiles(localFileSystem, testingBaseDirPath)
        .map(f =>
          fileStatusToResult(f._1)
        ) should contain theSameElementsAs (sourceOnlyResult ++ destOnlyResult)

      spark.stop()

    }

    it("perform distcp with non-update/non-overwrite and logging") {

      val spark = SparkSession.builder().master("local[*]").getOrCreate()

      val input = files()

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

object TestSparkDistCP {
  def listFiles(fs: FileSystem, path: Path): Seq[(SerializableFileStatus, Seq[SerializableFileStatus])]
  = FileListUtils.listFiles(fs, path, 10, false, List.empty, FailMissingDirectoryAction)
}