package com.coxautodata.objects

import org.apache.spark.Partitioner

/**
  * Custom partitioner based on the indexes array containing (partitionid, number of batches within partition)
  * Will handle missing partitions.
  */
case class CopyPartitioner(indexes: Array[(Int, Int)]) extends Partitioner {

  private val indexesAsMap = indexes.toMap

  override val numPartitions: Int = indexes.map(_._2).sum + indexes.length

  val partitionOffsets: Map[Int, Int] = {
    indexes.scanRight((-1, numPartitions)) { case ((partition, maxKey), (_, previousOffset)) => (partition, previousOffset - maxKey - 1) }.dropRight(1).toMap
  }

  override def getPartition(key: Any): Int = key match {
    case (p: Int, i: Int) =>
      if (!indexesAsMap.keySet.contains(p)) throw new RuntimeException(s"Key partition $p of key [($p, $i)] was not found in the indexes [${indexesAsMap.keySet.mkString(", ")}].")
      else if (i > indexesAsMap(p)) throw new RuntimeException(s"Key index $i of key [($p, $i)] is outside range [<=${indexesAsMap(p)}].")
      else partitionOffsets(p) + i
    case u => throw new RuntimeException(s"Partitioned does not support key [$u]. Must be (Int, Int).")
  }

}
