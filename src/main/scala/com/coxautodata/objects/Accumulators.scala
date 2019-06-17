package com.coxautodata.objects

import org.apache.spark.sql.SparkSession
import org.apache.spark.util.LongAccumulator

class Accumulators(sparkSession: SparkSession) {

  def handleResult(result: DistCPResult): Unit = result match {
    case DeleteResult(_, DeleteActionResult.SkippedDoesNotExists) => ???
    case DeleteResult(_, DeleteActionResult.SkippedDryRun) => ???
    case DeleteResult(_, DeleteActionResult.Deleted) => ???
    case DeleteResult(_, DeleteActionResult.Failed(e)) => ???
    case DirectoryCopyResult(_, _, CopyActionResult.SkippedAlreadyExists) => ???
    case DirectoryCopyResult(_, _, CopyActionResult.SkippedDryRun) => ???
    case DirectoryCopyResult(_, _, CopyActionResult.Created) => ???
    case DirectoryCopyResult(_, _, CopyActionResult.Failed(e)) => ???
    case FileCopyResult(_, _, l, CopyActionResult.SkippedAlreadyExists | CopyActionResult.SkippedIdenticalFileAlreadyExists | CopyActionResult.SkippedDryRun ) =>
      filesSkipped.add(1)
      bytesSkipped.add(l)
    case FileCopyResult(_, _, l, CopyActionResult.Copied) =>
      filesCopied.add(1)
      bytesCopied.add(l)
    case FileCopyResult(_, _, l, CopyActionResult.OverwrittenOrUpdated) =>
      filesCopied.add(1)
      bytesCopied.add(l)
      filesUpdatedOrOverwritten.add(1)
    case FileCopyResult(_, _, l, CopyActionResult.Failed(e)) =>
      filesFailed.add(1)
      exceptionCount.addException(e)

  }

  val bytesCopied: LongAccumulator = sparkSession.sparkContext.longAccumulator("BytesCopied")
  //
  val bytesSkipped: LongAccumulator = sparkSession.sparkContext.longAccumulator("BytesSkipped")
  //
  //  val foldersCreated: LongAccumulator = sparkSession.sparkContext.longAccumulator("FoldersCreated")
  //
    val filesCopied: LongAccumulator = sparkSession.sparkContext.longAccumulator("FilesCopied")
  //
  //  val foldersSkipped: LongAccumulator = sparkSession.sparkContext.longAccumulator("FoldersSkipped")
  //
  val filesSkipped: LongAccumulator = sparkSession.sparkContext.longAccumulator("FilesSkipped")
  val filesFailed: LongAccumulator = sparkSession.sparkContext.longAccumulator("FilesFailed")
  //
  val filesUpdatedOrOverwritten: LongAccumulator = sparkSession.sparkContext.longAccumulator("FilesUpdatedOrOverwritten")

  val exceptionCount: ExceptionCountAccumulator = new ExceptionCountAccumulator
  sparkSession.sparkContext.register(exceptionCount, "ExceptionCount")
}