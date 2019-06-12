package com.coxautodata.objects

import org.scalatest.{FunSpec, Matchers}

class TestCopyPartitioner extends FunSpec with Matchers {

  describe("getCopyPartitioner") {

    it("produce a valid partitioner") {

      val input = Array(0 -> 2, 1 -> 1, 2 -> 0, 3 -> 1)
      val partitioner = CopyPartitioner(input)

      partitioner.partitionOffsets should contain theSameElementsAs Map(
        0 -> 0,
        1 -> 3,
        2 -> 5,
        3 -> 6
      )

      partitioner.numPartitions should be(8)

      partitioner.getPartition((3, 1)) should be(7)

      intercept[RuntimeException] {
        partitioner.getPartition(1)
      }.getMessage should be("Partitioned does not support key [1]. Must be (Int, Int).")

      intercept[RuntimeException] {
        partitioner.getPartition((4, 1))
      }.getMessage should be("Key partition 4 of key [(4, 1)] was not found in the indexes [0, 1, 2, 3].")

      intercept[RuntimeException] {
        partitioner.getPartition((2, 1))
      }.getMessage should be("Key index 1 of key [(2, 1)] is outside range [<=0].")

    }

    it("partitioner with missing partitions") {

      val input = Array(0 -> 2, 1 -> 1, 3 -> 1)
      val partitioner = CopyPartitioner(input)

      partitioner.partitionOffsets should contain theSameElementsAs Map(
        0 -> 0,
        1 -> 3,
        3 -> 5
      )

      partitioner.numPartitions should be(7)

      partitioner.getPartition((0, 0)) should be(0)
      partitioner.getPartition((0, 1)) should be(1)
      partitioner.getPartition((0, 2)) should be(2)
      partitioner.getPartition((1, 0)) should be(3)
      partitioner.getPartition((1, 1)) should be(4)
      intercept[RuntimeException] {
        partitioner.getPartition((2, 0))
      }.getMessage should be("Key partition 2 of key [(2, 0)] was not found in the indexes [0, 1, 3].")
      partitioner.getPartition((3, 0)) should be(5)
      partitioner.getPartition((3, 1)) should be(6)

    }

  }

}
