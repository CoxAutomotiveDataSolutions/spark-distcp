package com.coxautodata.objects

import java.net.URI

import com.coxautodata.SparkDistCP.KeyedCopyDefinition

/**
  * Definition of a single copy
  *
  * @param source      Source file/folder to copy
  * @param destination Destination to copy to
  */
case class SingleCopyDefinition(source: SerializableFileStatus, destination: URI)

/**
  * Definition of a copy that includes any copying of parent folders this file/folder depends on
  *
  * @param source           Source file/folder to copy
  * @param destination      Destination to copy to
  * @param dependentFolders Any dependent folder copies this file/folder depends on
  */
case class CopyDefinitionWithDependencies(source: SerializableFileStatus, destination: URI, dependentFolders: Seq[SingleCopyDefinition]) {

  def toKeyedDefinition: KeyedCopyDefinition = (destination, this)

  def getAllCopyDefinitions: Seq[SingleCopyDefinition] = dependentFolders :+ SingleCopyDefinition(source, destination)
}