package com.coxautodata

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import java.io.IOException
import java.net.URI
import scala.util.matching.Regex

/** Options for the DistCP application See [[OptionsParsing.parse]] for the
  * explanation of each option
  */
case class SparkDistCPOptions(
  update: Boolean = SparkDistCPOptions.Defaults.update,
  overwrite: Boolean = SparkDistCPOptions.Defaults.overwrite,
  delete: Boolean = SparkDistCPOptions.Defaults.delete,
  log: Option[URI] = SparkDistCPOptions.Defaults.log,
  ignoreErrors: Boolean = SparkDistCPOptions.Defaults.ignoreErrors,
  dryRun: Boolean = SparkDistCPOptions.Defaults.dryRun,
  consistentPathBehaviour: Boolean =
    SparkDistCPOptions.Defaults.consistentPathBehaviour,
  maxFilesPerTask: Int = SparkDistCPOptions.Defaults.maxFilesPerTask,
  maxBytesPerTask: Long = SparkDistCPOptions.Defaults.maxBytesPerTask,
  filters: Option[URI] = SparkDistCPOptions.Defaults.filters,
  filterNot: List[Regex] = SparkDistCPOptions.Defaults.filterNot,
  numListstatusThreads: Int = SparkDistCPOptions.Defaults.numListstatusThreads,
  verbose: Boolean = SparkDistCPOptions.Defaults.verbose
) {

  val updateOverwritePathBehaviour: Boolean =
    !consistentPathBehaviour && (update || overwrite)

  def validateOptions(): Unit = {
    assert(maxFilesPerTask > 0, "maxFilesPerTask must be positive")

    assert(maxBytesPerTask > 0, "maxBytesPerTask must be positive")

    assert(numListstatusThreads > 0, "numListstatusThreads must be positive")

    assert(
      !(update && overwrite),
      "Both update and overwrite cannot be specified"
    )

    assert(
      !(delete && !overwrite && !update),
      "Delete must be specified with either overwrite or update"
    )
  }

  def withFiltersFromFile(
    hadoopConfiguration: Configuration
  ): SparkDistCPOptions = {

    val fn = filters
      .map(f => {
        try {
          val path = new Path(f)
          val fs = path.getFileSystem(hadoopConfiguration)

          val in = fs.open(path)

          val r = scala.io.Source.fromInputStream(in).getLines().map(_.r).toList

          in.close()
          r
        } catch {
          case e: IOException =>
            throw new RuntimeException("Invalid filter file " + f, e)
        }
      })
      .getOrElse(List.empty)

    this.copy(filterNot = fn)

  }

}

object SparkDistCPOptions {

  object Defaults {
    val update: Boolean = false
    val overwrite: Boolean = false
    val delete: Boolean = false
    val log: Option[URI] = None
    val ignoreErrors: Boolean = false
    val dryRun: Boolean = false
    val consistentPathBehaviour: Boolean = false
    val maxFilesPerTask: Int = 1000
    val maxBytesPerTask: Long = 1073741824L
    val filters: Option[URI] = None
    val filterNot: List[Regex] = List.empty
    val numListstatusThreads: Int = 10
    val verbose: Boolean = false
  }

}
