package com.coxautodata.objects

import org.apache.log4j.{Level, LogManager, Logger}

trait Logging {

  // Method to get the logger name for this object
  protected def logName: String = {
    // Ignore trailing $'s in the class names for Scala objects
    this.getClass.getName.stripSuffix("$")
  }

  private val log: Logger = LogManager.getLogger(logName)

  // Set logger level
  protected def setLogLevel(level: Level): Unit = log.setLevel(level)

  // Log methods that take only a String
  protected def logInfo(msg: => String) {
    if (log.isInfoEnabled) log.info(msg)
  }

  protected def logDebug(msg: => String) {
    if (log.isDebugEnabled) log.debug(msg)
  }

  protected def logTrace(msg: => String) {
    if (log.isTraceEnabled) log.trace(msg)
  }

  protected def logWarning(msg: => String) {
    log.warn(msg)
  }

  protected def logError(msg: => String) {
    log.error(msg)
  }

  // Log methods that take Throwables (Exceptions/Errors) too
  protected def logInfo(msg: => String, throwable: Throwable) {
    if (log.isInfoEnabled) log.info(msg, throwable)
  }

  protected def logDebug(msg: => String, throwable: Throwable) {
    if (log.isDebugEnabled) log.debug(msg, throwable)
  }

  protected def logTrace(msg: => String, throwable: Throwable) {
    if (log.isTraceEnabled) log.trace(msg, throwable)
  }

  protected def logWarning(msg: => String, throwable: Throwable) {
    log.warn(msg, throwable)
  }

  protected def logError(msg: => String, throwable: Throwable) {
    log.error(msg, throwable)
  }

  protected def isTraceEnabled: Boolean = {
    log.isTraceEnabled
  }


}
