package com.coxautodata.utils

import com.coxautodata.objects.DeleteActionResult.{
  Deleted,
  SkippedDoesNotExists,
  SkippedDryRun
}
import com.coxautodata.objects._
import com.coxautodata.utils.CopyUtils._
import com.coxautodata.{SparkDistCPOptions, TestSpec}
import org.apache.hadoop.fs.Path

class TestCopyUtils extends TestSpec {

  describe("filesAreIdentical") {

    it("differing lengths") {
      val one = createFile(new Path("1.file"), "a".getBytes)
      val two = createFile(new Path("2.file"), "aa".getBytes)
      filesAreIdentical(
        one,
        Option(localFileSystem.getFileChecksum(one.getPath)),
        two,
        Option(localFileSystem.getFileChecksum(two.getPath))
      ) should be(false)
    }

    it("same file") {
      val one = createFile(new Path("1.file"), "a".getBytes)
      val two = createFile(new Path("2.file"), "a".getBytes)
      filesAreIdentical(
        one,
        Option(localFileSystem.getFileChecksum(one.getPath)),
        two,
        Option(localFileSystem.getFileChecksum(two.getPath))
      ) should be(true)
    }

  }

  describe("performCopy") {

    it("successful copy") {

      val source = createFile(new Path("1.file"), "a".getBytes)
      val destPath = new Path(testingBaseDirPath, "2.file")
      performCopy(
        localFileSystem,
        source,
        localFileSystem,
        destPath.toUri,
        removeExisting = false,
        ignoreErrors = false,
        taskAttemptID = 1
      )

      filesAreIdentical(
        source,
        Option(localFileSystem.getFileChecksum(source.getPath)),
        SerializableFileStatus(localFileSystem.getFileStatus(destPath)),
        Option(localFileSystem.getFileChecksum(destPath))
      ) should be(true)

    }

    it("successful copy with overwrite") {

      val source = createFile(new Path("1.file"), "a".getBytes)
      val destPath = new Path(testingBaseDirPath, "2.file")

      createFile(new Path(destPath.getName), "aa".getBytes)

      filesAreIdentical(
        source,
        Option(localFileSystem.getFileChecksum(source.getPath)),
        SerializableFileStatus(localFileSystem.getFileStatus(destPath)),
        Option(localFileSystem.getFileChecksum(destPath))
      ) should be(false)

      performCopy(
        localFileSystem,
        source,
        localFileSystem,
        destPath.toUri,
        removeExisting = true,
        ignoreErrors = false,
        taskAttemptID = 1
      )

      filesAreIdentical(
        source,
        Option(localFileSystem.getFileChecksum(source.getPath)),
        SerializableFileStatus(localFileSystem.getFileStatus(destPath)),
        Option(localFileSystem.getFileChecksum(destPath))
      ) should be(true)

    }

    it("failed rename") {
      val source = createFile(new Path("1.file"), "a".getBytes)
      val destPath = new Path(testingBaseDirPath, "2.file")

      createFile(new Path(destPath.getName), "aa".getBytes)

      intercept[RuntimeException] {
        performCopy(
          localFileSystem,
          source,
          localFileSystem,
          destPath.toUri,
          removeExisting = false,
          ignoreErrors = false,
          taskAttemptID = 1
        )
      }

      performCopy(
        localFileSystem,
        source,
        localFileSystem,
        destPath.toUri,
        removeExisting = false,
        ignoreErrors = true,
        taskAttemptID = 2
      ).getMessage should be(
        s"Source: [${source.getPath.toUri}], Destination: [$destPath], " +
          s"Type: [FileCopy: 1 bytes], Result: [Failed: Cannot create file [$destPath] as it already exists]"
      )

    }

  }

  describe("createDirectory") {

    it("create a directory") {
      val sourcePath = new Path(testingBaseDirPath, "sub")
      localFileSystem.mkdirs(sourcePath)
      val destPath = new Path(testingBaseDirPath, "dest")
      val copyDefinition = SingleCopyDefinition(
        SerializableFileStatus(localFileSystem.getFileStatus(sourcePath)),
        destPath.toUri
      )

      // Dry run
      localFileSystem.exists(destPath) should be(false)
      createDirectory(
        localFileSystem,
        copyDefinition,
        SparkDistCPOptions(dryRun = true)
      )
      localFileSystem.exists(destPath) should be(false)

      // Create
      localFileSystem.exists(destPath) should be(false)
      createDirectory(
        localFileSystem,
        copyDefinition,
        SparkDistCPOptions()
      ).copyAction should be(CopyActionResult.Created)
      localFileSystem.exists(destPath) should be(true)

      // Already exists
      createDirectory(
        localFileSystem,
        copyDefinition,
        SparkDistCPOptions()
      ).copyAction should be(CopyActionResult.SkippedAlreadyExists)
    }

  }

  describe("copyFile") {

    it("successful copy") {

      val source = createFile(new Path("1.file"), "a".getBytes)
      val destPath = new Path(testingBaseDirPath, "2.file")
      val copyDefinition = SingleCopyDefinition(source, destPath.toUri)

      // Dry run
      copyFile(
        localFileSystem,
        localFileSystem,
        copyDefinition,
        SparkDistCPOptions(dryRun = true),
        1
      ).copyAction should be(CopyActionResult.SkippedDryRun)
      localFileSystem.exists(destPath) should be(false)

      // Missing at destination
      copyFile(
        localFileSystem,
        localFileSystem,
        copyDefinition,
        SparkDistCPOptions(),
        1
      ).copyAction should be(CopyActionResult.Copied)
      localFileSystem.exists(destPath) should be(true)

      filesAreIdentical(
        source,
        Option(localFileSystem.getFileChecksum(source.getPath)),
        SerializableFileStatus(localFileSystem.getFileStatus(destPath)),
        Option(localFileSystem.getFileChecksum(destPath))
      ) should be(true)

      // Present at destination, skipped
      localFileSystem.delete(destPath, false)
      createFile(new Path("2.file"), "aa".getBytes).getPath
      filesAreIdentical(
        source,
        Option(localFileSystem.getFileChecksum(source.getPath)),
        SerializableFileStatus(localFileSystem.getFileStatus(destPath)),
        Option(localFileSystem.getFileChecksum(destPath))
      ) should be(false)

      copyFile(
        localFileSystem,
        localFileSystem,
        copyDefinition,
        SparkDistCPOptions(),
        1
      ).copyAction should be(CopyActionResult.SkippedAlreadyExists)

      filesAreIdentical(
        source,
        Option(localFileSystem.getFileChecksum(source.getPath)),
        SerializableFileStatus(localFileSystem.getFileStatus(destPath)),
        Option(localFileSystem.getFileChecksum(destPath))
      ) should be(false)

    }

    it("successful copy with overwrite") {
      val source = createFile(new Path("1.file"), "a".getBytes)
      val destPath = createFile(new Path("2.file"), "aa".getBytes).getPath
      val copyDefinition = SingleCopyDefinition(source, destPath.toUri)

      filesAreIdentical(
        source,
        Option(localFileSystem.getFileChecksum(source.getPath)),
        SerializableFileStatus(localFileSystem.getFileStatus(destPath)),
        Option(localFileSystem.getFileChecksum(destPath))
      ) should be(false)

      // Present at destination, overwrite dry-run
      copyFile(
        localFileSystem,
        localFileSystem,
        copyDefinition,
        SparkDistCPOptions(overwrite = true, dryRun = true),
        1
      ).copyAction should be(CopyActionResult.SkippedDryRun)

      filesAreIdentical(
        source,
        Option(localFileSystem.getFileChecksum(source.getPath)),
        SerializableFileStatus(localFileSystem.getFileStatus(destPath)),
        Option(localFileSystem.getFileChecksum(destPath))
      ) should be(false)

      // Present at destination, overwrite
      copyFile(
        localFileSystem,
        localFileSystem,
        copyDefinition,
        SparkDistCPOptions(overwrite = true),
        1
      ).copyAction should be(CopyActionResult.OverwrittenOrUpdated)

      filesAreIdentical(
        source,
        Option(localFileSystem.getFileChecksum(source.getPath)),
        SerializableFileStatus(localFileSystem.getFileStatus(destPath)),
        Option(localFileSystem.getFileChecksum(destPath))
      ) should be(true)
    }

    it("successful copy with update") {
      val source = createFile(new Path("1.file"), "a".getBytes)
      val destPath = createFile(new Path("2.file"), "aa".getBytes).getPath
      val copyDefinition = SingleCopyDefinition(source, destPath.toUri)

      filesAreIdentical(
        source,
        Option(localFileSystem.getFileChecksum(source.getPath)),
        SerializableFileStatus(localFileSystem.getFileStatus(destPath)),
        Option(localFileSystem.getFileChecksum(destPath))
      ) should be(false)

      // Present at destination, update dry-run
      copyFile(
        localFileSystem,
        localFileSystem,
        copyDefinition,
        SparkDistCPOptions(update = true, dryRun = true),
        1
      ).copyAction should be(CopyActionResult.SkippedDryRun)

      filesAreIdentical(
        source,
        Option(localFileSystem.getFileChecksum(source.getPath)),
        SerializableFileStatus(localFileSystem.getFileStatus(destPath)),
        Option(localFileSystem.getFileChecksum(destPath))
      ) should be(false)

      // Present at destination, differing file, update
      copyFile(
        localFileSystem,
        localFileSystem,
        copyDefinition,
        SparkDistCPOptions(update = true),
        1
      ).copyAction should be(CopyActionResult.OverwrittenOrUpdated)

      filesAreIdentical(
        source,
        Option(localFileSystem.getFileChecksum(source.getPath)),
        SerializableFileStatus(localFileSystem.getFileStatus(destPath)),
        Option(localFileSystem.getFileChecksum(destPath))
      ) should be(true)

      copyFile(
        localFileSystem,
        localFileSystem,
        copyDefinition,
        SparkDistCPOptions(update = true),
        1
      ).copyAction should be(CopyActionResult.SkippedIdenticalFileAlreadyExists)
    }

    it("failed copy with sub directory") {

      val source = createFile(new Path("1.file"), "a".getBytes)
      val destPath = new Path(testingBaseDirPath, new Path("sub", "1.file"))
      val copyDefinition = SingleCopyDefinition(source, destPath.toUri)

      intercept[RuntimeException] {
        copyFile(
          localFileSystem,
          localFileSystem,
          copyDefinition,
          SparkDistCPOptions(),
          1
        )
      }

      copyFile(
        localFileSystem,
        localFileSystem,
        copyDefinition,
        SparkDistCPOptions(ignoreErrors = true),
        1
      ).getMessage should be(
        s"Source: [${source.getPath.toUri}], " +
          s"Destination: [$destPath], Type: [FileCopy: 1 bytes], " +
          s"Result: [Failed: Destination folder [${destPath.getParent}] does not exist]"
      )

    }
  }

  describe("handleCopy") {

    it("successful copy with sub directory") {

      val source = createFile(new Path("sub", "1.file"), "a".getBytes)
      val destPath = new Path(testingBaseDirPath, new Path("sub1", "1.file"))
      val parentCopyDefinition = SingleCopyDefinition(
        SerializableFileStatus(
          localFileSystem.getFileStatus(source.getPath.getParent)
        ),
        new Path(testingBaseDirPath, "sub1").toUri
      )
      val fileCopyDefinition = CopyDefinitionWithDependencies(
        source,
        destPath.toUri,
        Seq(parentCopyDefinition)
      )
      val options = SparkDistCPOptions()

      fileCopyDefinition.getAllCopyDefinitions.foreach(
        handleCopy(localFileSystem, localFileSystem, _, options, 1)
      )

      filesAreIdentical(
        source,
        Option(localFileSystem.getFileChecksum(source.getPath)),
        SerializableFileStatus(localFileSystem.getFileStatus(destPath)),
        Option(localFileSystem.getFileChecksum(destPath))
      ) should be(true)

    }

  }

  describe("deleteFile") {

    it("delete directory that does not exist") {

      val path = new Path(testingBaseDirPath, "sub")
      deleteFile(localFileSystem, path, SparkDistCPOptions()) should be(
        DeleteResult(path.toUri, SkippedDoesNotExists)
      )

    }

    it("delete a directory containing a file") {

      val filePath = createFile(new Path("sub/file"), "a".getBytes).getPath
      val dirPath = filePath.getParent

      localFileSystem.exists(filePath) should be(true)
      localFileSystem.exists(dirPath) should be(true)
      deleteFile(
        localFileSystem,
        dirPath,
        SparkDistCPOptions(dryRun = true)
      ) should be(DeleteResult(dirPath.toUri, SkippedDryRun))
      localFileSystem.exists(filePath) should be(true)
      localFileSystem.exists(dirPath) should be(true)
      deleteFile(localFileSystem, dirPath, SparkDistCPOptions()) should be(
        DeleteResult(dirPath.toUri, Deleted)
      )
      localFileSystem.exists(filePath) should be(false)
      localFileSystem.exists(dirPath) should be(false)
    }

  }

}
