package com.coxautodata.objects

import java.net.URI

import com.coxautodata.utils.PathUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem

import scala.collection.mutable

/**
  * FileSystem caching class. Aims to prevent many of the same FileSystem objects being created
  * between copy and delete actions in the same partition.
  */
class FileSystemObjectCacher(hadoopConfiguration: Configuration) {

  private val cache: mutable.Map[URI, FileSystem] = mutable.Map.empty

  /**
    * Get a FileSystem object based on the given URI if it already exists.
    * If it doesn't exist, create one and store it.
    */
  def getOrCreate(uri: URI): FileSystem = get(uri) match {
    case Some(fs) => fs
    case None =>
      val fs = FileSystem.get(uri, hadoopConfiguration)
      cache.update(fs.getUri, fs)
      fs
  }

  /**
    * Get a FileSystem object based on the given URI if it already exists.
    */
  def get(uri: URI): Option[FileSystem] = cache.collectFirst { case (u, f) if PathUtils.uriIsChild(u.resolve("/"), uri) => f }

}