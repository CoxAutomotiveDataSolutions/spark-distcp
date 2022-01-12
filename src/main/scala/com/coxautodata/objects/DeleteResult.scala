package com.coxautodata.objects

import java.net.URI

/** Result of the DistCP delete used for both logging to a logger and a file.
  */
case class DeleteResult(path: URI, actionResult: DeleteActionResult)
    extends DistCPResult {
  def getMessage: String =
    s"Path: [$path], Type: [Delete], Result: [${actionResult.message}]"
}

sealed trait DeleteActionResult extends Serializable {
  def message: String = this.getClass.getSimpleName
}

object DeleteActionResult {

  object SkippedDoesNotExists extends DeleteActionResult

  object SkippedDryRun extends DeleteActionResult

  object Deleted extends DeleteActionResult

  case class Failed(e: Throwable) extends DeleteActionResult {
    override def message: String = s"${super.message}: ${e.getMessage}"
  }

}
