package com.coxautodata.objects

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class TestChangeLoggerLevel extends AnyFunSpec with Matchers {

  it("test implementation") {
    val logging = new Object with Logging
    logging.setLogLevel("DEBUG")
    logging.log.isDebugEnabled should be(true)
    logging.log.isInfoEnabled should be(true)
    logging.setLogLevel("INFO")
    logging.log.isDebugEnabled should be(false)
    logging.log.isInfoEnabled should be(true)
  }
}
