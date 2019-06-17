package com.coxautodata.objects

import java.net.URI

/**
  * Result of the DistCP copy used for both logging to a logger and a file.
  */
trait CopyResult extends DistCPResult {
}

case class FileCopyResult(source: URI, destination: URI, len: Long, copyAction: FileCopyActionResult) extends CopyResult {
  def getMessage: String = s"Source: [$source], Destination: [$destination], Type: [FileCopy: $len bytes], Result: [${copyAction.message}]"
}

case class DirectoryCopyResult(source: URI, destination: URI, copyAction: DirectoryCreateActionResult) extends CopyResult {
  def getMessage: String = s"Source: [$source], Destination: [$destination], Type: [DirectoryCreate], Result: [${copyAction.message}]"
}

sealed trait CopyActionResult extends Serializable {
  def message: String = this.getClass.getSimpleName.stripSuffix("$")
}

sealed trait FileCopyActionResult extends CopyActionResult

sealed trait DirectoryCreateActionResult extends CopyActionResult

object CopyActionResult {

  object SkippedAlreadyExists extends FileCopyActionResult with DirectoryCreateActionResult

  object SkippedIdenticalFileAlreadyExists extends FileCopyActionResult

  object SkippedDryRun extends FileCopyActionResult with DirectoryCreateActionResult

  object Created extends DirectoryCreateActionResult

  object Copied extends FileCopyActionResult

  object OverwrittenOrUpdated extends FileCopyActionResult

  case class Failed(e: Throwable) extends FileCopyActionResult with DirectoryCreateActionResult {
    override def message: String = s"${super.message}: ${e.getMessage}"
  }

}

