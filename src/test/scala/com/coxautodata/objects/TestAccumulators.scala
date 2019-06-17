package com.coxautodata.objects

import java.io.FileNotFoundException

import org.apache.spark.sql.SparkSession
import org.scalatest.{FunSpec, Matchers}

class TestAccumulators extends FunSpec with Matchers {

  it("test all accumulator conditions") {

    val spark = SparkSession.builder().master("local[*]").getOrCreate()

    val testCases: Seq[DistCPResult] = List(
      DeleteResult(null, DeleteActionResult.SkippedDoesNotExists),
      DeleteResult(null, DeleteActionResult.SkippedDryRun),
      DeleteResult(null, DeleteActionResult.Deleted),
      DeleteResult(null, DeleteActionResult.Failed(new RuntimeException("test"))),
      DirectoryCopyResult(null, null, CopyActionResult.SkippedAlreadyExists),
      DirectoryCopyResult(null, null, CopyActionResult.SkippedDryRun),
      DirectoryCopyResult(null, null, CopyActionResult.Created),
      DirectoryCopyResult(null, null, CopyActionResult.Failed(new RuntimeException("test"))),
      FileCopyResult(null, null, 1, CopyActionResult.SkippedAlreadyExists),
      FileCopyResult(null, null, 1000, CopyActionResult.SkippedIdenticalFileAlreadyExists),
      FileCopyResult(null, null, 1000000, CopyActionResult.SkippedDryRun),
      FileCopyResult(null, null, 1000000, CopyActionResult.Copied),
      FileCopyResult(null, null, 1000, CopyActionResult.OverwrittenOrUpdated),
      FileCopyResult(null, null, 50000, CopyActionResult.Failed(new FileNotFoundException("test")))
    )

    val acc = new Accumulators(spark)

    testCases.foreach(acc.handleResult)

    acc.getOutputText should be(
      """--Raw data--
        |Data copied: 977 KB (1001000 bytes)
        |Data skipped (already existing files, dry-run and failures): 1 MB (1051001 bytes)
        |--Files--
        |Files copied (new files and overwritten/updated files): 2
        |Files overwritten/updated: 1
        |Skipped files for copying (already existing files, dry-run and failures): 4
        |Failed files during copy: 1
        |--Folders--
        |Folders created: 1
        |Skipped folder creates (already existing folders, dry-run and failures): 3
        |Failed folder creates: 1
        |--Deletes--
        |Successful delete operations: 1
        |Skipped delete operations (files/folders already missing, dry-run and failures): 3
        |Failed delete operations: 1
        |--Exception counts--
        |java.lang.RuntimeException: 2
        |java.io.FileNotFoundException: 1""".stripMargin
    )

    spark.stop()

  }

}