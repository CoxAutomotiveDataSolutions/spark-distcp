package com.coxautodata.utils

import java.io.FileNotFoundException
import java.net.URI

import com.coxautodata.SparkDistCPOptions
import com.coxautodata.objects._
import org.apache.hadoop.fs._
import org.apache.hadoop.io.IOUtils
import org.apache.log4j.Level

import scala.util.{Failure, Success, Try}

object CopyUtils extends Logging {

  /**
    * Handle the copy of a file/folder
    *
    * @param sourceFS      Source FileSystem object
    * @param destFS        Destination FileSystem object
    * @param definition    Definition of the copy
    * @param options       SparkDistCP options
    * @param taskAttemptID Spark task attempt ID (used to create a unique temporary file)
    */
  def handleCopy(sourceFS: FileSystem, destFS: FileSystem, definition: SingleCopyDefinition, options: SparkDistCPOptions, taskAttemptID: Long): DistCPResult = {

    if (options.verbose) setLogLevel(Level.DEBUG)

    val r = {
      if (definition.source.isDirectory) {
        CopyUtils.createDirectory(destFS, definition, options)
      }
      else if (definition.source.isFile) {
        CopyUtils.copyFile(sourceFS, destFS, definition, options, taskAttemptID)
      }
      else
        throw new UnsupportedOperationException(s"Given file is neither file nor directory. Copy unsupported: ${definition.source.getPath}")
    }

    logInfo(r.getMessage)
    r
  }

  /**
    * Handle the delete of a file/folder
    *
    * @param fs      FileSystem to delete the file from
    * @param uri     URI of file/path
    * @param options DistCP options
    */
  def handleDelete(fs: FileSystem, uri: URI, options: SparkDistCPOptions): DeleteResult = {

    if (options.verbose) setLogLevel(Level.DEBUG)

    val path = new Path(uri)

    val r = deleteFile(fs, path, options)
    logInfo(r.getMessage)
    r

  }

  /**
    * Internal delete function
    */
  private[utils] def deleteFile(fs: FileSystem, path: Path, options: SparkDistCPOptions): DeleteResult = {
    if (!fs.exists(path)) {
      DeleteResult(path.toUri, DeleteActionResult.SkippedDoesNotExists)
    }
    else if (options.dryRun) {
      DeleteResult(path.toUri, DeleteActionResult.SkippedDryRun)
    }
    else {
      Try(fs.delete(path, true)) match {
        case Success(true) => DeleteResult(path.toUri, DeleteActionResult.Deleted)
        case Success(false) if !fs.exists(path) => DeleteResult(path.toUri, DeleteActionResult.SkippedDoesNotExists)
        case Success(false) if options.ignoreErrors => DeleteResult(path.toUri, DeleteActionResult.Failed(new RuntimeException(s"Failed to delete directory [$path].")))
        case Success(false) => throw new RuntimeException(s"Failed to delete directory [$path].")
        case Failure(e) if options.ignoreErrors => DeleteResult(path.toUri, DeleteActionResult.Failed(e))
        case Failure(e) => throw e
      }
    }
  }

  /**
    * Internal create directory function
    */
  private[utils] def createDirectory(destFS: FileSystem, definition: SingleCopyDefinition, options: SparkDistCPOptions): DirectoryCopyResult = {
    val destPath = new Path(definition.destination)
    if (destFS.exists(destPath)) {
      DirectoryCopyResult(definition.source.getPath.toUri, definition.destination, CopyActionResult.SkippedAlreadyExists)
    }
    else if (options.dryRun) {
      DirectoryCopyResult(definition.source.getPath.toUri, definition.destination, CopyActionResult.SkippedDryRun)
    }
    else {
      val result = Try {
        if (destFS.exists(destPath.getParent)) {
          destFS.mkdirs(destPath)
          DirectoryCopyResult(definition.source.getPath.toUri, definition.destination, CopyActionResult.Created)
        }
        else throw new FileNotFoundException(s"Parent folder [${destPath.getParent}] does not exist.")
      }
        .recover {
          case _: FileAlreadyExistsException =>
            DirectoryCopyResult(definition.source.getPath.toUri, definition.destination, CopyActionResult.SkippedAlreadyExists)
        }
      result match {
        case Success(v) => v
        case Failure(e) if options.ignoreErrors =>
          logError(s"Exception whilst creating directory [${definition.destination}]", e)
          DirectoryCopyResult(definition.source.getPath.toUri, definition.destination, CopyActionResult.Failed(e))
        case Failure(e) =>
          throw e
      }
    }
  }

  /**
    * Internal copy file function
    */
  private[utils] def copyFile(sourceFS: FileSystem, destFS: FileSystem, definition: SingleCopyDefinition, options: SparkDistCPOptions, taskAttemptID: Long): FileCopyResult = {
    val destPath = new Path(definition.destination)
    Try(destFS.getFileStatus(destPath)) match {
      case Failure(_: FileNotFoundException) if options.dryRun =>
        FileCopyResult(definition.source.getPath.toUri, definition.destination, definition.source.len, CopyActionResult.SkippedDryRun)
      case Failure(_: FileNotFoundException) =>
        performCopy(sourceFS, definition.source, destFS, definition.destination, removeExisting = false, ignoreErrors = options.ignoreErrors, taskAttemptID)
      case Failure(e) if options.ignoreErrors =>
        logError(s"Exception whilst getting destination file information [${definition.destination}]", e)
        FileCopyResult(definition.source.getPath.toUri, definition.destination, definition.source.len, CopyActionResult.Failed(e))
      case Failure(e) =>
        throw e
      case Success(_) if options.overwrite && options.dryRun =>
        FileCopyResult(definition.source.getPath.toUri, definition.destination, definition.source.len, CopyActionResult.SkippedDryRun)
      case Success(_) if options.overwrite =>
        performCopy(sourceFS, definition.source, destFS, definition.destination, removeExisting = true, ignoreErrors = options.ignoreErrors, taskAttemptID)
      case Success(d) if options.update =>
        Try {
          filesAreIdentical(
            definition.source,
            Option(sourceFS.getFileChecksum(definition.source.getPath)),
            SerializableFileStatus(d),
            Option(destFS.getFileChecksum(destPath))
          )
        }
        match {
          case Failure(e) if options.ignoreErrors =>
            logError(s"Exception whilst getting source and destination checksum: source [${definition.source.getPath}] destination [${definition.destination}", e)
            FileCopyResult(definition.source.getPath.toUri, definition.destination, definition.source.len, CopyActionResult.Failed(e))
          case Failure(e) =>
            throw e
          case Success(true) =>
            FileCopyResult(definition.source.getPath.toUri, definition.destination, definition.source.len, CopyActionResult.SkippedIdenticalFileAlreadyExists)
          case Success(false) if options.dryRun =>
            FileCopyResult(definition.source.getPath.toUri, definition.destination, definition.source.len, CopyActionResult.SkippedDryRun)
          case Success(false) =>
            performCopy(sourceFS, definition.source, destFS, definition.destination, removeExisting = true, ignoreErrors = options.ignoreErrors, taskAttemptID)
        }
      case Success(_) =>
        FileCopyResult(definition.source.getPath.toUri, definition.destination, definition.source.len, CopyActionResult.SkippedAlreadyExists)
    }
  }

  /**
    * Check whether two files match, based on length and checksum.
    * If either of the checksums are None, then checksums are not used for comparison.
    */
  private[utils] def filesAreIdentical(f1: SerializableFileStatus, mc1: => Option[FileChecksum], f2: SerializableFileStatus, mc2: => Option[FileChecksum]): Boolean = {
    if (f1.getLen != f2.getLen) {
      logDebug(s"Length [${f1.getLen}] of file [${f1.uri}] was not the same as length [${f2.getLen}] of file [${f2.uri}]. Files are not identical.")
      false
    }
    else {
      val c1 = mc1
      val c2 = mc2
      val same = mc1.flatMap(c1 => mc2.map(c1 ==)).getOrElse(true)
      if (same) {
        logDebug(s"CRC [$c1] of file [${f1.uri}] was the same as CRC [$c2] of file [${f2.uri}]. Files are identical.")
        true
      }
      else {
        logDebug(s"CRC [$c1] of file [${f1.uri}] was not the same as CRC [$c2] of file [${f2.uri}]. Files are not identical.")
        false
      }

    }

  }

  /**
    * Internal copy function
    * Only pass in true for removeExisting if the file actually exists
    */
  def performCopy(sourceFS: FileSystem, sourceFile: SerializableFileStatus, destFS: FileSystem, dest: URI, removeExisting: Boolean, ignoreErrors: Boolean, taskAttemptID: Long): FileCopyResult = {

    val destPath = new Path(dest)

    val tempPath = new Path(destPath.getParent, s".sparkdistcp.$taskAttemptID.${destPath.getName}")

    Try {
      var in: Option[FSDataInputStream] = None
      var out: Option[FSDataOutputStream] = None
      try {
        in = Some(sourceFS.open(sourceFile.getPath))
        if (!destFS.exists(tempPath.getParent)) throw new RuntimeException(s"Destination folder [${tempPath.getParent}] does not exist")
        out = Some(destFS.create(tempPath, false))
        IOUtils.copyBytes(in.get, out.get, sourceFS.getConf.getInt("io.file.buffer.size", 4096))

      } catch {
        case e: Throwable => throw e
      } finally {
        in.foreach(_.close())
        out.foreach(_.close())
      }
    }.map {
      _ =>
        val tempFile = destFS.getFileStatus(tempPath)
        if (sourceFile.getLen != tempFile.getLen)
          throw new RuntimeException(s"Written file [${tempFile.getPath}] length [${tempFile.getLen}] did not match source file [${sourceFile.getPath}] length [${sourceFile.getLen}]")

        if (removeExisting) {
          val res = destFS.delete(destPath, false)
          if (!res) throw new RuntimeException(s"Failed to clean up existing file [$destPath]")
        }
        if (destFS.exists(destPath)) throw new RuntimeException(s"Cannot create file [$destPath] as it already exists")
        val res = destFS.rename(tempPath, destPath)
        if (!res) throw new RuntimeException(s"Failed to rename temporary file [$tempPath] to [$destPath]")
    } match {
      case Success(_) if removeExisting =>
        FileCopyResult(sourceFile.getPath.toUri, dest, sourceFile.len, CopyActionResult.OverwrittenOrUpdated)
      case Success(_) =>
        FileCopyResult(sourceFile.getPath.toUri, dest, sourceFile.len, CopyActionResult.Copied)
      case Failure(e) if ignoreErrors =>
        logError(s"Failed to copy file [${sourceFile.getPath}] to [$destPath]", e)
        FileCopyResult(sourceFile.getPath.toUri, dest, sourceFile.len, CopyActionResult.Failed(e))
      case Failure(e) =>
        throw e
    }

  }

}
