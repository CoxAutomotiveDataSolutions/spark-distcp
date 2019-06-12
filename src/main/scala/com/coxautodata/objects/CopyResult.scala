package com.coxautodata.objects

import java.net.URI

/**
  * Result of the DistCP copy used for both logging to a logger and a file.
  */
case class CopyResult(source: URI, destination: URI, copyType: ActionType, copyAction: CopyActionResult) extends DistCPResult {
  def getMessage: String = s"Source: [$source], Destination: [$destination], Type: [${copyType.message}], Result: [${copyAction.message}]"
}

sealed trait ActionType extends Serializable {
  def message: String = this.getClass.getSimpleName
}

object ActionType {

  object DirectoryCreate extends ActionType

  object FileCopy extends ActionType

}


sealed trait CopyActionResult extends Serializable {
  def message: String = this.getClass.getSimpleName
}

object CopyActionResult {

  object SkippedAlreadyExists extends CopyActionResult

  object SkippedIdenticalFileAlreadyExists extends CopyActionResult

  object SkippedDryRun extends CopyActionResult

  object Created extends CopyActionResult

  object Copied extends CopyActionResult

  case class Failed(e: Throwable) extends CopyActionResult {
    override def message: String = s"${super.message}: ${e.getMessage}"
  }

}

