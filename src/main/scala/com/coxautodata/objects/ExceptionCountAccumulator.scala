package com.coxautodata.objects

import java.util
import java.util.Collections
import java.util.function.{BiConsumer, BiFunction}

import org.apache.spark.util.AccumulatorV2

class ExceptionCountAccumulator extends AccumulatorV2[String, java.util.Map[String, Long]] {

  private val _map: java.util.Map[String, Long] = Collections.synchronizedMap(new util.HashMap[String, Long]())

  override def isZero: Boolean = _map.isEmpty

  override def copyAndReset(): ExceptionCountAccumulator = new ExceptionCountAccumulator

  override def copy(): ExceptionCountAccumulator = {
    val newAcc = new ExceptionCountAccumulator
    _map.synchronized {
      newAcc._map.putAll(_map)
    }
    newAcc
  }

  override def reset(): Unit = _map.clear()

  def add(e: Throwable): Unit = add(e.getClass.getName.stripSuffix("$"))

  override def add(k: String): Unit = {
    add(k, 1)
  }

  private def add(k: String, v: Long): Unit = {
    _map.merge(k, v, CombineCounts)
  }

  override def merge(other: AccumulatorV2[String, util.Map[String, Long]]): Unit = {
    other match {
      case e: ExceptionCountAccumulator =>
        e._map.forEach {
          new BiConsumer[String, Long] {
            override def accept(k: String, v: Long): Unit = add(k, v)
          }
        }
      case _ => throw new UnsupportedOperationException(
        s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
    }
  }

  override def value: util.Map[String, Long] = _map
}

object CombineCounts extends BiFunction[Long, Long, Long] {
  override def apply(t: Long, u: Long): Long = t + u
}

