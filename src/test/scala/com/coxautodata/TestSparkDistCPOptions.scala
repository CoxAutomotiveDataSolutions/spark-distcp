package com.coxautodata

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class TestSparkDistCPOptions extends AnyFunSpec with Matchers {

  it("updateOverwritePathBehaviour"){

    SparkDistCPOptions().updateOverwritePathBehaviour should be (false)
    SparkDistCPOptions(consistentPathBehaviour = true).updateOverwritePathBehaviour should be (false)
    SparkDistCPOptions(update = true, consistentPathBehaviour = true).updateOverwritePathBehaviour should be (false)
    SparkDistCPOptions(overwrite = true, consistentPathBehaviour = true).updateOverwritePathBehaviour should be (false)
    SparkDistCPOptions(update = true).updateOverwritePathBehaviour should be (true)
    SparkDistCPOptions(overwrite = true).updateOverwritePathBehaviour should be (true)

  }

}
