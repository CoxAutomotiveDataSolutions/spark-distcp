package com.coxautodata

import org.scalatest.{FunSpec, Matchers}

class TestSparkDistCPOptions extends FunSpec with Matchers {

  it("updateOverwritePathBehaviour"){

    SparkDistCPOptions().updateOverwritePathBehaviour should be (false)
    SparkDistCPOptions(consistentPathBehaviour = true).updateOverwritePathBehaviour should be (false)
    SparkDistCPOptions(update = true, consistentPathBehaviour = true).updateOverwritePathBehaviour should be (false)
    SparkDistCPOptions(overwrite = true, consistentPathBehaviour = true).updateOverwritePathBehaviour should be (false)
    SparkDistCPOptions(update = true).updateOverwritePathBehaviour should be (true)
    SparkDistCPOptions(overwrite = true).updateOverwritePathBehaviour should be (true)

  }

}
