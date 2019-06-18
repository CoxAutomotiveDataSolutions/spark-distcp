package com.coxautodata.utils

import org.scalatest.{FunSpec, Matchers}

class TestFileUtils extends FunSpec with Matchers {

  it("byteCountToDisplaySize") {

    import java.math.BigInteger
    val b1023 = BigInteger.valueOf(1023)
    val b1025 = BigInteger.valueOf(1025)
    val KB1 = BigInteger.valueOf(1024)
    val MB1 = KB1.multiply(KB1)
    val GB1 = MB1.multiply(KB1)
    val GB2 = GB1.add(GB1)
    val TB1 = GB1.multiply(KB1)
    val PB1 = TB1.multiply(KB1)
    val EB1 = PB1.multiply(KB1)
    FileUtils.byteCountToDisplaySize(BigInteger.ZERO) should be("0 bytes")
    FileUtils.byteCountToDisplaySize(BigInteger.ONE) should be("1 bytes")
    FileUtils.byteCountToDisplaySize(b1023) should be("1023 bytes")
    FileUtils.byteCountToDisplaySize(KB1) should be("1 KB (1024 bytes)")
    FileUtils.byteCountToDisplaySize(b1025) should be("1 KB (1025 bytes)")
    FileUtils.byteCountToDisplaySize(MB1.subtract(BigInteger.ONE)) should be("1023 KB (1048575 bytes)")
    FileUtils.byteCountToDisplaySize(MB1) should be("1 MB (1048576 bytes)")
    FileUtils.byteCountToDisplaySize(MB1.add(BigInteger.ONE)) should be("1 MB (1048577 bytes)")
    FileUtils.byteCountToDisplaySize(GB1.subtract(BigInteger.ONE)) should be("1023 MB (1073741823 bytes)")
    FileUtils.byteCountToDisplaySize(GB1) should be("1 GB (1073741824 bytes)")
    FileUtils.byteCountToDisplaySize(GB1.add(BigInteger.ONE)) should be("1 GB (1073741825 bytes)")
    FileUtils.byteCountToDisplaySize(GB2) should be("2 GB (2147483648 bytes)")
    FileUtils.byteCountToDisplaySize(GB2.subtract(BigInteger.ONE)) should be("1.99 GB (2147483647 bytes)")
    FileUtils.byteCountToDisplaySize(TB1) should be("1 TB (1099511627776 bytes)")
    FileUtils.byteCountToDisplaySize(PB1) should be("1 PB (1125899906842624 bytes)")
    FileUtils.byteCountToDisplaySize(EB1) should be("1 EB (1152921504606846976 bytes)")
    FileUtils.byteCountToDisplaySize(java.lang.Long.MAX_VALUE) should be("7.99 EB (9223372036854775807 bytes)")
    // Other MAX_VALUEs
    FileUtils.byteCountToDisplaySize(BigInteger.valueOf(Character.MAX_VALUE)) should be("63.9 KB (65535 bytes)")
    FileUtils.byteCountToDisplaySize(BigInteger.valueOf(java.lang.Short.MAX_VALUE)) should be("31.9 KB (32767 bytes)")
    FileUtils.byteCountToDisplaySize(BigInteger.valueOf(Integer.MAX_VALUE)) should be("1.99 GB (2147483647 bytes)")

  }

}
