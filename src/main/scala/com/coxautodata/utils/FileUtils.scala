package com.coxautodata.utils

import java.math.{BigDecimal, BigInteger}

// Adapted from: https://jira.apache.org/jira/secure/attachment/12542305/roundedByteCountToDisplaySize.patch
object FileUtils {

  val ONE_KB = 1024
  val ONE_KB_BI: BigInteger = BigInteger.valueOf(ONE_KB)
  val ONE_MB: Long = ONE_KB * ONE_KB
  val ONE_MB_BI: BigInteger = ONE_KB_BI.multiply(ONE_KB_BI)
  val ONE_GB: Long = ONE_KB * ONE_MB
  val ONE_GB_BI: BigInteger = ONE_KB_BI.multiply(ONE_MB_BI)
  val ONE_TB: Long = ONE_KB * ONE_GB
  val ONE_TB_BI: BigInteger = ONE_KB_BI.multiply(ONE_GB_BI)
  val ONE_PB: Long = ONE_KB * ONE_TB
  val ONE_PB_BI: BigInteger = ONE_KB_BI.multiply(ONE_TB_BI)
  val ONE_EB: Long = ONE_KB * ONE_PB
  val ONE_EB_BI: BigInteger = ONE_KB_BI.multiply(ONE_PB_BI)
  val ONE_ZB: BigInteger =
    BigInteger.valueOf(ONE_KB).multiply(BigInteger.valueOf(ONE_EB))
  val ONE_YB: BigInteger = ONE_KB_BI.multiply(ONE_ZB)

  def byteCountToDisplaySize(size: BigInteger): String = {
    val sizeBD = new BigDecimal(size)
    if (size.divide(ONE_YB).compareTo(BigInteger.ZERO) > 0)
      getThreeSigFigs(sizeBD.divide(new BigDecimal(ONE_YB))) + s" YB (${String.valueOf(size)} bytes)"
    else if (size.divide(ONE_ZB).compareTo(BigInteger.ZERO) > 0)
      getThreeSigFigs(sizeBD.divide(new BigDecimal(ONE_ZB))) + s" ZB (${String.valueOf(size)} bytes)"
    else if (size.divide(ONE_EB_BI).compareTo(BigInteger.ZERO) > 0)
      getThreeSigFigs(
        sizeBD.divide(new BigDecimal(ONE_EB_BI))
      ) + s" EB (${String.valueOf(size)} bytes)"
    else if (size.divide(ONE_PB_BI).compareTo(BigInteger.ZERO) > 0)
      getThreeSigFigs(
        sizeBD.divide(new BigDecimal(ONE_PB_BI))
      ) + s" PB (${String.valueOf(size)} bytes)"
    else if (size.divide(ONE_TB_BI).compareTo(BigInteger.ZERO) > 0)
      getThreeSigFigs(
        sizeBD.divide(new BigDecimal(ONE_TB_BI))
      ) + s" TB (${String.valueOf(size)} bytes)"
    else if (size.divide(ONE_GB_BI).compareTo(BigInteger.ZERO) > 0)
      getThreeSigFigs(
        sizeBD.divide(new BigDecimal(ONE_GB_BI))
      ) + s" GB (${String.valueOf(size)} bytes)"
    else if (size.divide(ONE_MB_BI).compareTo(BigInteger.ZERO) > 0)
      getThreeSigFigs(
        sizeBD.divide(new BigDecimal(ONE_MB_BI))
      ) + s" MB (${String.valueOf(size)} bytes)"
    else if (size.divide(ONE_KB_BI).compareTo(BigInteger.ZERO) > 0)
      getThreeSigFigs(
        sizeBD.divide(new BigDecimal(ONE_KB_BI))
      ) + s" KB (${String.valueOf(size)} bytes)"
    else String.valueOf(size) + " bytes"
  }

  def byteCountToDisplaySize(size: Long): String = byteCountToDisplaySize(
    BigInteger.valueOf(size)
  )

  private def getThreeSigFigs(size: BigDecimal): String = {
    val (isDecimal, _, sizeS) = size.toString.foldLeft((false, 0, "")) {
      case ((decimal, count, agg), c) =>
        if (c == '.' && !decimal) (true, count, agg + c)
        else if (count < 3 || !decimal) (decimal, count + 1, agg + c)
        else (decimal, count + 1, agg)
    }

    if (isDecimal)
      sizeS.reverse.dropWhile(c => c == '0').reverse.stripSuffix(".")
    else sizeS

  }

}
