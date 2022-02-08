package com.coxautodata

import com.coxautodata.utils.DirectoryUtils.missingDirectoryAction
import com.coxautodata.utils.{DirectoryUtils, MissingDirectoryAction}

import java.net.URI
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

object OptionsParsing {

  /** Parse a set of command-line arguments into a [[Config]] object
    */
  def parse(args: Array[String]): Config = {

    val parser = new scopt.OptionParser[Config]("") {
      opt[Unit]("i")
        .action((_, c) => c.copyOptions(_.copy(ignoreErrors = true)))
        .text("Ignore failures")

      opt[String]("log")
        .action((log, c) => c.copyOptions(_.copy(log = Some(new URI(log)))))
        .text("Write logs to a URI")

      opt[Unit]("dryrun")
        .action((_, c) => c.copyOptions(_.copy(dryRun = true)))
        .text("Perform a trial run with no changes made")

      opt[Unit]("verbose")
        .action((_, c) => c.copyOptions(_.copy(verbose = true)))
        .text("Run in verbose mode")

      opt[Unit]("overwrite")
        .action((_, c) => c.copyOptions(_.copy(overwrite = true)))
        .text("Overwrite destination")

      opt[Unit]("update")
        .action((_, c) => c.copyOptions(_.copy(update = true)))
        .text("Overwrite if source and destination differ in size, or checksum")

      opt[String]("filters")
        .action((f, c) => c.copyOptions(_.copy(filters = Some(new URI(f)))))
        .text(
          "The path to a file containing a list of pattern strings, one string per line, such that paths matching the pattern will be excluded from the copy."
        )

      opt[Unit]("delete")
        .action((_, c) => c.copyOptions(_.copy(delete = true)))
        .text("Delete the files existing in the dst but not in src")

      opt[Int]("numListstatusThreads")
        .action((i, c) => c.copyOptions(_.copy(numListstatusThreads = i)))
        .text("Number of threads to use for building file listing")

      opt[Unit]("consistentPathBehaviour")
        .action((_, c) => c.copyOptions(_.copy(consistentPathBehaviour = true)))
        .text(
          "Revert the path behaviour when using overwrite or update to the path behaviour of non-overwrite/non-update"
        )

      opt[Int]("maxFilesPerTask")
        .action((i, c) => c.copyOptions(_.copy(maxFilesPerTask = i)))
        .text("Maximum number of files to copy in a single Spark task")

      opt[Long]("maxBytesPerTask")
        .action((i, c) => c.copyOptions(_.copy(maxBytesPerTask = i)))
        .text("Maximum number of bytes to copy in a single Spark task")

      opt[String]("onMissingDirectory")
        .action((s, c) => c.copyOptions(_.copy(missingDirectoryAction = missingDirectoryAction(s))))
        .text("Behaviour when source or target does not exist: either fail (default), create or log")

      help("help").text("prints this usage text")

      arg[String]("[source_path...] <target_path>")
        .unbounded()
        .action((u, c) => c.copy(URIs = c.URIs :+ new URI(u)))

    }

    parser.parse(args, Config()) match {
      case Some(config) =>
        config.validateUris()
        config.options.validateOptions()
        config
      case _ =>
        throw new RuntimeException("Failed to parse arguments")
    }
  }
}

case class Config(
  options: SparkDistCPOptions = SparkDistCPOptions(),
  URIs: Seq[URI] = Seq.empty
) {

  def copyOptions(f: SparkDistCPOptions => SparkDistCPOptions): Config = {
    this.copy(options = f(options))
  }

  def validateUris(): Unit = {
    require(
      URIs.length >= 2,
      "you must supply two or more paths, representing the source paths and a destination"
    )
  }

  def sourceAndDestPaths: (Seq[Path], Path) = {
    URIs.reverse match {
      case d :: s :: ts =>
        ((s :: ts).reverse.map(u => new Path(u)), new Path(d))
      case _ => throw new RuntimeException("Incorrect number of URIs")
    }
  }

}
