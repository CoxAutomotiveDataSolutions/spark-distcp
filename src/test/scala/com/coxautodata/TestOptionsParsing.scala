package com.coxautodata

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.scalatest.{FunSpec, Matchers}

class TestOptionsParsing extends FunSpec with Matchers {

  describe("Successful parsing") {

    it("default options one source") {

      val conf = OptionsParsing.parse(Array("src", "dest"), new Configuration())
      conf.sourceAndDestPaths should be(Seq(new Path("src")), new Path("dest"))
      conf.options should be(SparkDistCPOptions())

    }

    it("default options two sources") {

      val conf = OptionsParsing.parse(Array("src1", "src2", "dest"), new Configuration())
      conf.sourceAndDestPaths should be(Seq(new Path("src1"), new Path("src2")), new Path("dest"))
      conf.options should be(SparkDistCPOptions())

    }

    it("ignore failures flag") {

      val conf = OptionsParsing.parse(Array("--i", "src", "dest"), new Configuration())
      conf.sourceAndDestPaths should be(Seq(new Path("src")), new Path("dest"))
      conf.options should be(SparkDistCPOptions(ignoreErrors = true))

    }

    it("log option") {

      val conf = OptionsParsing.parse(Array("--log", "log", "src", "dest"), new Configuration())
      conf.sourceAndDestPaths should be(Seq(new Path("src")), new Path("dest"))
      conf.options should be(SparkDistCPOptions(log = Some(new URI("log"))))

    }

    it("dry-run flag") {

      val conf = OptionsParsing.parse(Array("--dryrun", "src", "dest"), new Configuration())
      conf.sourceAndDestPaths should be(Seq(new Path("src")), new Path("dest"))
      conf.options should be(SparkDistCPOptions(dryRun = true))

    }

    it("verbose flag") {

      val conf = OptionsParsing.parse(Array("--verbose", "src", "dest"), new Configuration())
      conf.sourceAndDestPaths should be(Seq(new Path("src")), new Path("dest"))
      conf.options should be(SparkDistCPOptions(verbose = true))

    }

    it("overwrite flag") {

      val conf = OptionsParsing.parse(Array("--overwrite", "src", "dest"), new Configuration())
      conf.sourceAndDestPaths should be(Seq(new Path("src")), new Path("dest"))
      conf.options should be(SparkDistCPOptions(overwrite = true))

    }

    it("update flag") {

      val conf = OptionsParsing.parse(Array("--update", "src", "dest"), new Configuration())
      conf.sourceAndDestPaths should be(Seq(new Path("src")), new Path("dest"))
      conf.options should be(SparkDistCPOptions(update = true))

    }

    it("filters flag") {

      val filtersFile = this.getClass.getResource("test.filters").getPath
      val conf = OptionsParsing.parse(Array("--filters", filtersFile, "src", "dest"), new Configuration())
      conf.sourceAndDestPaths should be(Seq(new Path("src")), new Path("dest"))
      conf.options.copy(filterNot = List.empty) should be(SparkDistCPOptions())
      conf.options.filterNot.map(_.toString()) should be(List(".*/_temporary($|/.*)", ".*/_committed.*", ".*/_started.*", ".*/_SUCCESS.*"))

    }

    it("delete flag") {

      val conf = OptionsParsing.parse(Array("--delete", "--update", "src", "dest"), new Configuration())
      conf.sourceAndDestPaths should be(Seq(new Path("src")), new Path("dest"))
      conf.options should be(SparkDistCPOptions(delete = true, update = true))

    }

    it("numListstatusThreads option") {

      val conf = OptionsParsing.parse(Array("--numListstatusThreads", "3", "src", "dest"), new Configuration())
      conf.sourceAndDestPaths should be(Seq(new Path("src")), new Path("dest"))
      conf.options should be(SparkDistCPOptions(numListstatusThreads = 3))

    }

    it("consistentPathBehaviour option") {

      val conf = OptionsParsing.parse(Array("--consistentPathBehaviour", "src", "dest"), new Configuration())
      conf.sourceAndDestPaths should be(Seq(new Path("src")), new Path("dest"))
      conf.options should be(SparkDistCPOptions(consistentPathBehaviour = true))

    }

    it("maxFilesPerTask option") {

      val conf = OptionsParsing.parse(Array("--maxFilesPerTask", "3", "src", "dest"), new Configuration())
      conf.sourceAndDestPaths should be(Seq(new Path("src")), new Path("dest"))
      conf.options should be(SparkDistCPOptions(maxFilesPerTask = 3))

    }

    it("maxBytesPerTask option") {

      val conf = OptionsParsing.parse(Array("--maxBytesPerTask", "30000000", "src", "dest"), new Configuration())
      conf.sourceAndDestPaths should be(Seq(new Path("src")), new Path("dest"))
      conf.options should be(SparkDistCPOptions(maxBytesPerTask = 30000000))

    }

  }

  describe("Failure parsing") {

    it("single path") {

      intercept[RuntimeException] {
        OptionsParsing.parse(Array("path"), new Configuration())
      }.getMessage should be("Failed to parse arguments")

    }

    it("missing filters file") {

      intercept[RuntimeException] {
        OptionsParsing.parse(Array("--filters", "none", "src", "dest"), new Configuration())
      }.getMessage should be("Failed to parse arguments")

    }

    it("negative max files") {

      intercept[java.lang.AssertionError] {
        OptionsParsing.parse(Array("--maxFilesPerTask", "-2", "src", "dest"), new Configuration())
      }.getMessage should be("assertion failed: maxFilesPerTask must be positive")

    }

    it("negative max bytes") {

      intercept[java.lang.AssertionError] {
        OptionsParsing.parse(Array("--maxBytesPerTask", "-2", "src", "dest"), new Configuration())
      }.getMessage should be("assertion failed: maxBytesPerTask must be positive")

    }

    it("negative num list status threads") {

      intercept[java.lang.AssertionError] {
        OptionsParsing.parse(Array("--numListstatusThreads", "-2", "src", "dest"), new Configuration())
      }.getMessage should be("assertion failed: numListstatusThreads must be positive")

    }

    it("both update and overwrite specified") {

      intercept[java.lang.AssertionError] {
        OptionsParsing.parse(Array("--update", "--overwrite", "src", "dest"), new Configuration())
      }.getMessage should be("assertion failed: Both update and overwrite cannot be specified")

    }

    it("delete specified without update or overwrite") {

      intercept[java.lang.AssertionError] {
        OptionsParsing.parse(Array("--delete", "src", "dest"), new Configuration())
      }.getMessage should be("assertion failed: Delete must be specified with either overwrite or update")

    }

  }

}
