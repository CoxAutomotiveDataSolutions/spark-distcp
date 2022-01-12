package com.coxautodata.objects

import java.net.URI

import org.apache.hadoop.fs.{FileStatus, Path}

/** Case class to represent a simple status of a File. Exists because
  * [[FileStatus]] is not serializable
  */
case class SerializableFileStatus(uri: URI, len: Long, fileType: FileType)
    extends Serializable {
  def getPath: Path = new Path(uri)

  def getLen: Long = len

  def isDirectory: Boolean = fileType == Directory

  def isFile: Boolean = fileType == File
}

object SerializableFileStatus {

  /** Create a [[SerializableFileStatus]] from a [[FileStatus]] object
    */
  def apply(fileStatus: FileStatus): SerializableFileStatus = {

    val fileType =
      if (fileStatus.isDirectory) Directory
      else if (fileStatus.isFile) File
      else
        throw new RuntimeException(
          s"File [$fileStatus] is neither a directory or file"
        )

    new SerializableFileStatus(
      fileStatus.getPath.toUri,
      fileStatus.getLen,
      fileType
    )
  }
}

sealed trait FileType extends Serializable

case object File extends FileType

case object Directory extends FileType
