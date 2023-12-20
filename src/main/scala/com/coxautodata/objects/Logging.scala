package com.coxautodata.objects

import org.slf4j.{Logger, LoggerFactory}

trait Logging {

  // Method to get the logger name for this object
  protected def logName: String = {
    // Ignore trailing $'s in the class names for Scala objects
    this.getClass.getName.stripSuffix("$")
  }

  private[objects] val log: Logger = LoggerFactory.getLogger(logName)

  // Set logger level
  protected[objects] def setLogLevel(level: String): Unit = {
    log.getClass.getName match {
      case "org.slf4j.impl.Log4jLoggerAdapter" =>
        val log4j1LevelClass = Class.forName("org.apache.log4j.Level")
        val log4j1Level = log4j1LevelClass.getField(level).get(null)
        val log4j1LoggerField = log.getClass.getDeclaredField("logger")
        log4j1LoggerField.setAccessible(true)
        val log4j1Logger = log4j1LoggerField.get(log)
        val setLevelMethod = log4j1Logger.getClass.getMethod("setLevel", log4j1LevelClass)
        setLevelMethod.invoke(log4j1Logger, log4j1Level)
      case "org.apache.logging.slf4j.Log4jLogger" =>
        val log4j2LevelClass = Class.forName("org.apache.logging.log4j.Level")
        val log4j2Level = log4j2LevelClass.getField(level).get(null)
        val log4j2LoggerField = log.getClass.getDeclaredField("logger")
        log4j2LoggerField.setAccessible(true)
        val log4j2Logger = log4j2LoggerField.get(log)
        val setLevelMethod = log4j2Logger.getClass.getMethod("setLevel", log4j2LevelClass)
        setLevelMethod.invoke(log4j2Logger, log4j2Level)
      case unsupported =>
        log.warn(
          s"Unsupported logging framework: $unsupported, setLogLevel does nothing"
        )
    }
  }

  // Log methods that take only a String
  protected def logInfo(msg: => String): Unit = {
    if (log.isInfoEnabled) log.info(msg)
  }

  protected def logDebug(msg: => String): Unit = {
    if (log.isDebugEnabled) log.debug(msg)
  }

  protected def logTrace(msg: => String): Unit = {
    if (log.isTraceEnabled) log.trace(msg)
  }

  protected def logWarning(msg: => String): Unit = {
    log.warn(msg)
  }

  protected def logError(msg: => String): Unit = {
    log.error(msg)
  }

  // Log methods that take Throwables (Exceptions/Errors) too
  protected def logInfo(msg: => String, throwable: Throwable): Unit = {
    if (log.isInfoEnabled) log.info(msg, throwable)
  }

  protected def logDebug(msg: => String, throwable: Throwable): Unit = {
    if (log.isDebugEnabled) log.debug(msg, throwable)
  }

  protected def logTrace(msg: => String, throwable: Throwable): Unit = {
    if (log.isTraceEnabled) log.trace(msg, throwable)
  }

  protected def logWarning(msg: => String, throwable: Throwable): Unit = {
    log.warn(msg, throwable)
  }

  protected def logError(msg: => String, throwable: Throwable): Unit = {
    log.error(msg, throwable)
  }

  protected def isTraceEnabled: Boolean = {
    log.isTraceEnabled
  }

}
