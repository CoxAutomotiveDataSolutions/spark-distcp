package com.coxautodata.objects

/** Result of the DistCP action (copy/delete) used for both logging to a logger
  * and a file.
  */
trait DistCPResult extends Serializable {

  def getMessage: String

}
