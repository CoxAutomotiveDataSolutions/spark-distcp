package com.coxautodata

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

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
    uri: URI,
    hadoopConfiguration: Configuration
  ): SparkDistCPOptions = {

    val path = new Path(uri)
    val fs = path.getFileSystem(hadoopConfiguration)

    val in = fs.open(path)

    val r = scala.io.Source.fromInputStream(in).getLines().map(_.r).toList

    in.close()

    this.copy(filterNot = r)

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
    val filterNot: List[Regex] = List.empty
    val numListstatusThreads: Int = 10
    val verbose: Boolean = false
  }

}
