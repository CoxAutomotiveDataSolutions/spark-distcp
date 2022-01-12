package com.coxautodata.objects

import java.util

import org.apache.spark.util.AccumulatorV2
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._

class TestExceptionCountAccumulator extends AnyFunSpec with Matchers {

  it("test implementation") {

    val acc = new ExceptionCountAccumulator

    acc.add(new RuntimeException)
    acc.add("TestException")

    acc.value.asScala should contain theSameElementsAs Map("TestException" -> 1, "java.lang.RuntimeException" -> 1)

    val accCopy = acc.copy()
    acc.value.asScala should contain theSameElementsAs Map("TestException" -> 1, "java.lang.RuntimeException" -> 1)
    acc.value.asScala should contain theSameElementsAs accCopy.value.asScala

    acc.merge(accCopy)
    acc.value.asScala should contain theSameElementsAs Map("TestException" -> 2, "java.lang.RuntimeException" -> 2)

    acc.reset()
    acc.isZero should be(true)
    acc.value.isEmpty should be(true)

    intercept[UnsupportedOperationException] {
      acc.merge(new TestAcc)
    }.getMessage should be("Cannot merge com.coxautodata.objects.ExceptionCountAccumulator with com.coxautodata.objects.TestAcc")

  }

}

class TestAcc extends AccumulatorV2[String, java.util.Map[String, Long]] {
  override def isZero: Boolean = ???

  override def copy(): AccumulatorV2[String, util.Map[String, Long]] = ???

  override def reset(): Unit = ???

  override def add(v: String): Unit = ???

  override def merge(other: AccumulatorV2[String, util.Map[String, Long]]): Unit = ???

  override def value: util.Map[String, Long] = ???
}