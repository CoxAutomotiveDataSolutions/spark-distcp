package com.coxautodata.utils

import com.coxautodata.objects.Logging
import com.coxautodata.utils.DirectoryUtils.NO_FILES
import org.apache.hadoop.fs.{FileSystem, LocatedFileStatus, Path, RemoteIterator}

import java.io.FileNotFoundException

abstract sealed class MissingDirectoryAction extends Logging {
  def requireDirectory(destFS: FileSystem, destPath: Path): Unit

  def createDirectory(destFS: FileSystem, destPath: Path): Unit = destFS.mkdirs(destPath)

  def listFiles(destFS: FileSystem, destPath: Path): RemoteIterator[LocatedFileStatus] = {
    if (destFS.exists(destPath)) {
      destFS.listLocatedStatus(destPath)
    } else {
      logWarning(s"Folder [${destPath.getParent}] does not exist.")
      NO_FILES
    }
  }

}

case object CreateMissingDirectoryAction extends MissingDirectoryAction {
  override def requireDirectory(destFS: FileSystem, destPath: Path): Unit = {
    if (!destFS.exists(destPath)) {
      destFS.mkdirs(destPath)
    }
  }
}

case object FailMissingDirectoryAction extends MissingDirectoryAction {
  override def requireDirectory(destFS: FileSystem, destPath: Path): Unit = {
    if (!destFS.exists(destPath)) {
      throw new FileNotFoundException(s"Folder [${destPath}] does not exist.")
    }
  }

  override def listFiles(destFS: FileSystem, destPath: Path): RemoteIterator[LocatedFileStatus] = {
    if (destFS.exists(destPath)) {
      destFS.listLocatedStatus(destPath)
    } else {
      throw new FileNotFoundException(s"Folder [${destPath}] does not exist.")
    }
  }
}

case object LogMissingDirectoryAction extends MissingDirectoryAction {
  override def requireDirectory(destFS: FileSystem, destPath: Path): Unit = {
    if (!destFS.exists(destPath)) {
      logWarning(s"Folder [${destPath.getParent}] does not exist.")
    }
  }

  override def createDirectory(destFS: FileSystem, destPath: Path): Unit = {
    logDebug(s"Do not create folder [${destPath}].")
  }
}

private class EmptyRemoteIterator[E] extends RemoteIterator[E] {
  override def hasNext: Boolean = false

  override def next(): E = throw new UnsupportedOperationException()
}

object DirectoryUtils {
  val NO_FILES: RemoteIterator[LocatedFileStatus] = new EmptyRemoteIterator[LocatedFileStatus]()

  def missingDirectoryAction(name: String): MissingDirectoryAction = name.trim.toLowerCase match {
    case "create" => CreateMissingDirectoryAction
    case "log" => LogMissingDirectoryAction
    case "fail" => FailMissingDirectoryAction
    case _ => throw new IllegalArgumentException(s"Invalid missing directory option [$name]")
  }

}
