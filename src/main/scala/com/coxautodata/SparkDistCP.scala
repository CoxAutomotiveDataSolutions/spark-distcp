package com.coxautodata

import java.net.URI

import com.coxautodata.objects._
import com.coxautodata.utils.{CopyUtils, FileListUtils, PathUtils}
import org.apache.hadoop.fs._
import org.apache.log4j.Level
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.{HashPartitioner, TaskContext}

/** Spark-based DistCp application. [[SparkDistCP.main]] is the command-line
  * entry to the application and [[SparkDistCP.run]] is the programmatic API
  * entry to the application
  */
object SparkDistCP extends Logging {

  type KeyedCopyDefinition = (URI, CopyDefinitionWithDependencies)

  /** Main entry point for command-line. Arguments are currently: Usage:
    * SparkDistCP [options] [source_path...] <target_path>
    *
    * --i Ignore failures
    * --log <value> Write logs to a URI
    * --dryrun Perform a trial run with no changes made
    * --verbose Run in verbose mode
    * --overwrite Overwrite destination
    * --update Overwrite if source and destination differ in size, or checksum
    * --filters <value> The path to a file containing a list of pattern strings,
    * one string per line, such that paths matching the pattern will be excluded
    * from the copy.
    * --delete Delete the files existing in the dst but not in src
    * --numListstatusThreads <value> Number of threads to use for building file
    * listing
    * --consistentPathBehaviour Revert the path behaviour when using overwrite
    * or update to the path behaviour of non-overwrite/non-update
    * --maxFilesPerTask <value> Maximum number of files to copy in a single
    * Spark task
    * --maxBytesPerTask <value> Maximum number of bytes to copy in a single
    * Spark task
    * --help prints this usage text [source_path...] <target_path>
    */
  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder().getOrCreate()

    val config =
      OptionsParsing.parse(args, sparkSession.sparkContext.hadoopConfiguration)

    val (src, dest) = config.sourceAndDestPaths
    run(sparkSession, src, dest, config.options)

  }

  /** Main entry point for programmatic access to the application.
    *
    * @param sparkSession
    *   Active Spark Session
    * @param sourcePaths
    *   Source paths to copy from
    * @param destinationPath
    *   Destination path to copy to
    * @param options
    *   Options to use in the application
    */
  def run(
    sparkSession: SparkSession,
    sourcePaths: Seq[Path],
    destinationPath: Path,
    options: SparkDistCPOptions
  ): Unit = {
    import sparkSession.implicits._

    assert(sourcePaths.nonEmpty, "At least one source path must be given")
    options.validateOptions()

    if (options.verbose) {
      sparkSession.sparkContext.setLogLevel("DEBUG")
      setLogLevel(Level.DEBUG)
    }

    val qualifiedSourcePaths = sourcePaths.map(
      PathUtils
        .pathToQualifiedPath(sparkSession.sparkContext.hadoopConfiguration, _)
    )
    val qualifiedDestinationPath = PathUtils.pathToQualifiedPath(
      sparkSession.sparkContext.hadoopConfiguration,
      destinationPath
    )

    val sourceRDD = FileListUtils.getSourceFiles(
      sparkSession.sparkContext,
      qualifiedSourcePaths.map(_.toUri),
      qualifiedDestinationPath.toUri,
      options.updateOverwritePathBehaviour,
      options.numListstatusThreads,
      options.filterNot
    )
    val destinationRDD = FileListUtils.getDestinationFiles(
      sparkSession.sparkContext,
      qualifiedDestinationPath,
      options
    )

    val joined = sourceRDD.fullOuterJoin(destinationRDD)

    val toCopy = joined.collect { case (_, (Some(s), _)) => s }

    val accumulators = new Accumulators(sparkSession)

    val copyResult: RDD[DistCPResult] = doCopy(toCopy, accumulators, options)

    val deleteResult: RDD[DistCPResult] = {
      if (options.delete) {
        val toDelete = joined.collect { case (d, (None, _)) => d }
        doDelete(toDelete, accumulators, options)
      } else {
        sparkSession.sparkContext.emptyRDD[DistCPResult]
      }
    }

    val allResults = copyResult union deleteResult

    options.log match {
      case None => allResults.foreach(_ => ())
      case Some(f) =>
        allResults
          .repartition(1)
          .map(_.getMessage)
          .toDS()
          .write
          .mode(SaveMode.Append)
          .csv(f.toString)
    }

    logInfo("SparkDistCP Run Statistics\n" + accumulators.getOutputText)

  }

  /** Perform the copy portion of the DistCP
    */
  private[coxautodata] def doCopy(
    sourceRDD: RDD[CopyDefinitionWithDependencies],
    accumulators: Accumulators,
    options: SparkDistCPOptions
  ): RDD[DistCPResult] = {

    val serConfig = new ConfigSerDeser(
      sourceRDD.sparkContext.hadoopConfiguration
    )
    batchAndPartitionFiles(
      sourceRDD,
      options.maxFilesPerTask,
      options.maxBytesPerTask
    )
      .mapPartitions { iterator =>
        val hadoopConfiguration = serConfig.get()
        val attemptID = TaskContext.get().taskAttemptId()
        val fsCache = new FileSystemObjectCacher(hadoopConfiguration)

        iterator
          .flatMap(_._2.getAllCopyDefinitions)
          .collectMapWithEmptyCollection(
            (d, z) => z.contains(d),
            d => {
              val r = CopyUtils.handleCopy(
                fsCache.getOrCreate(d.source.uri),
                fsCache.getOrCreate(d.destination),
                d,
                options,
                attemptID
              )
              accumulators.handleResult(r)
              r
            }
          )
      }
  }

  /** Perform the delete from destination portion of the DistCP
    */
  private[coxautodata] def doDelete(
    destRDD: RDD[URI],
    accumulators: Accumulators,
    options: SparkDistCPOptions
  ): RDD[DistCPResult] = {
    val serConfig = new ConfigSerDeser(destRDD.sparkContext.hadoopConfiguration)
    val count = destRDD.count()
    destRDD
      .repartition((count / options.maxFilesPerTask).toInt.max(1))
      .mapPartitions { iterator =>
        val hadoopConfiguration = serConfig.get()
        val fsCache = new FileSystemObjectCacher(hadoopConfiguration)
        iterator
          .collectMapWithEmptyCollection(
            (d, z) => z.exists(p => PathUtils.uriIsChild(p, d)),
            d => {
              val r = CopyUtils.handleDelete(fsCache.getOrCreate(d), d, options)
              accumulators.handleResult(r)
              r
            }
          )
      }
  }

  /** DistCP helper implicits on iterators
    */
  private[coxautodata] implicit class DistCPIteratorImplicit[B](
    iterator: Iterator[B]
  ) {

    /** Scan over an iterator, mapping as we go with `action`, but making a
      * decision on which objects to actually keep using a set of what objects
      * have been seen and the `skip` function. Similar to a combining `collect`
      * and `foldLeft`.
      *
      * @param skip
      *   Should a mapped version of this element not be included in the output
      * @param action
      *   Function to map the element
      * @return
      *   An iterator
      */
    def collectMapWithEmptyCollection(
      skip: (B, Set[B]) => Boolean,
      action: B => DistCPResult
    ): Iterator[DistCPResult] = {

      iterator
        .scanLeft((Set.empty[B], None: Option[DistCPResult])) {
          case ((z, _), d) if skip(d, z) => (z, None)
          case ((z, _), d) =>
            (z + d, Some(action(d)))
        }
        .collect { case (_, Some(r)) => r }

    }

  }

  /** Batch the given RDD into groups of files depending on
    * [[SparkDistCPOptions.maxFilesPerTask]] and
    * [[SparkDistCPOptions.maxBytesPerTask]] and repartition the RDD so files in
    * the same batches are in the same partitions
    */
  private[coxautodata] def batchAndPartitionFiles(
    rdd: RDD[CopyDefinitionWithDependencies],
    maxFilesPerTask: Int,
    maxBytesPerTask: Long
  ): RDD[((Int, Int), CopyDefinitionWithDependencies)] = {
    val partitioner =
      rdd.partitioner.getOrElse(new HashPartitioner(rdd.partitions.length))
    val sorted = rdd
      .map(v => (v.source.uri.toString, v))
      .repartitionAndSortWithinPartitions(partitioner)
      .map(_._2)
    val batched = sorted.mapPartitionsWithIndex(
      generateBatchedFileKeys(maxFilesPerTask, maxBytesPerTask)
    ) // sorted

    batched.partitionBy(CopyPartitioner(batched))
  }

  /** Key the RDD within partitions based on batches of files based on
    * [[SparkDistCPOptions.maxFilesPerTask]] and
    * [[SparkDistCPOptions.maxBytesPerTask]] thresholds
    */
  private[coxautodata] def generateBatchedFileKeys(
    maxFilesPerTask: Int,
    maxBytesPerTask: Long
  ): (Int, Iterator[CopyDefinitionWithDependencies]) => Iterator[
    ((Int, Int), CopyDefinitionWithDependencies)
  ] = { (partition, iterator) =>
    iterator
      .scanLeft[(Int, Int, Long, CopyDefinitionWithDependencies)](
        0,
        0,
        0,
        null
      ) { case ((index, count, bytes, _), definition) =>
        val newCount = count + 1
        val newBytes = bytes + definition.source.getLen
        if (newCount > maxFilesPerTask || newBytes > maxBytesPerTask) {
          (index + 1, 1, definition.source.getLen, definition)
        } else {
          (index, newCount, newBytes, definition)
        }
      }
      .drop(1)
      .map { case (index, _, _, file) => ((partition, index), file) }
  }

}
