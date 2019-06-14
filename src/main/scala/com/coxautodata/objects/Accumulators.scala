package com.coxautodata.objects

import com.coxautodata.objects.ActionType.{DirectoryCreate, FileCopy}
import org.apache.spark.sql.SparkSession

class Accumulators(sparkSession: SparkSession) {

  def handleResult(result: DistCPResult): Unit = result match {
    case DeleteResult(_, DeleteActionResult.SkippedDoesNotExists) => ???
    case DeleteResult(_, DeleteActionResult.SkippedDryRun) => ???
    case DeleteResult(_, DeleteActionResult.Deleted) => ???
    case DeleteResult(_, DeleteActionResult.Failed(e)) => ???
    case CopyResult(_, _, DirectoryCreate, CopyActionResult.SkippedAlreadyExists) => ???
    case CopyResult(_, _, DirectoryCreate, CopyActionResult.SkippedIdenticalFileAlreadyExists) => ???
    case CopyResult(_, _, DirectoryCreate, CopyActionResult.SkippedDryRun) => ???
    case CopyResult(_, _, DirectoryCreate, CopyActionResult.Created) => ???
    case CopyResult(_, _, DirectoryCreate, CopyActionResult.Copied) => ???
    case CopyResult(_, _, DirectoryCreate, CopyActionResult.Failed(e)) => ???
    case CopyResult(_, _, FileCopy, CopyActionResult.SkippedAlreadyExists) => ???
    case CopyResult(_, _, FileCopy, CopyActionResult.SkippedIdenticalFileAlreadyExists) => ???
    case CopyResult(_, _, FileCopy, CopyActionResult.SkippedDryRun) => ???
    case CopyResult(_, _, FileCopy, CopyActionResult.Created) => ???
    case CopyResult(_, _, FileCopy, CopyActionResult.Copied) => ???
    case CopyResult(_, _, FileCopy, CopyActionResult.Failed(e)) => ???

  }

  //  val bytesCopied: LongAccumulator = sparkSession.sparkContext.longAccumulator("BytesCopied")
  //
  //  val bytesSkipped: LongAccumulator = sparkSession.sparkContext.longAccumulator("BytesSkipped")
  //
  //  val foldersCreated: LongAccumulator = sparkSession.sparkContext.longAccumulator("FoldersCreated")
  //
  //  val filesCreated: LongAccumulator = sparkSession.sparkContext.longAccumulator("FilesCreated")
  //
  //  val foldersSkipped: LongAccumulator = sparkSession.sparkContext.longAccumulator("FoldersSkipped")
  //
  //  val filesSkipped: LongAccumulator = sparkSession.sparkContext.longAccumulator("FilesSkipped")
  //
  //  val filesUpdatedOrOverwritten: LongAccumulator = sparkSession.sparkContext.longAccumulator("FilesUpdatedOrOverwritten")

}