# SparkDistCP
[![Build Status](https://travis-ci.org/CoxAutomotiveDataSolutions/spark-distcp.svg?branch=master)](https://travis-ci.org/CoxAutomotiveDataSolutions/spark-distcp) 
[![Maven Central](https://img.shields.io/maven-central/v/com.coxautodata/spark-distcp_2.11.svg)](https://search.maven.org/search?q=g:com.coxautodata%20AND%20a:spark-distcp*) [![Coverage Status](https://img.shields.io/codecov/c/github/CoxAutomotiveDataSolutions/spark-distcp/master.svg)](https://codecov.io/gh/CoxAutomotiveDataSolutions/spark-distcp/branch/master)

## What is SparkDistCP?

SparkDistCP is an attempt at reimplementing [Hadoop DistCP](https://hadoop.apache.org/docs/current/hadoop-distcp/DistCp.html) in Apache Spark.

There are several reasons you might want to do this:
* Using DistCP in a Spark-only/non-YARN environment
* Reducing DistCP copy times by generating many smaller tasks therefore limiting long-running tasks/map tasks
* To use DistCP programmatically through Spark

**Note:** Not all features of Hadoop DistCP have been reimplemented yet. See [What is currently missing from SparkDistCP?](#what-is-currently-missing-from-sparkdistcp) for an overview on what has not yet been implemented.

**Further note:** SparkDistCP is in early development therefore you should use this library with caution! We provide absolutely no guarantee that this tool will not cause accidental data loss.

## How do I run SparkDistCP?

You can run SparkDistCP from the command-line using:
```shell
bin/spark-submit --packages com.coxautodata:spark-distcp_2.11:{{version}} --class com.coxautodata.SparkDistCP  "" --help
```

The empty string is needed here as `spark-submit` requires an application Jar to be specified however the Main class is in the dependency specified in `packages`.

The usage of the command-line arguments resembles that of the Hadoop DistCP:

```shell
Usage:  [options] [source_path...] <target_path>
```

Like Hadoop DistCP, SparkDistCP takes several options, one or more source paths and a target path.

SparkDistCP can also be invoked programmatically from a Spark shell in two way:

* By calling `main` directory and passing an array of command-line arguments:
```scala
import com.coxautodata.SparkDistCP
SparkDistCP.main(Array("--help"))
```

* Or using the typed API:
```scala
def run(sparkSession: SparkSession, sourcePaths: Seq[Path], destinationPath: Path, options: SparkDistCPOptions): Unit
```

For example:
```scala
import org.apache.hadoop.fs.Path
import com.coxautodata.{SparkDistCP, SparkDistCPOptions}
SparkDistCP.run(spark, Seq(new Path("hdfs://nn1:8020/foo/bar")), new Path("hdfs://nn2:8020/bar/foo"), SparkDistCPOptions(dryRun = true))
```

### Options:

| SparkDistCP Flag               | Equivalent Hadoop DistCP Flag | Description                                                                                                                                        | Notes                                                                                                                                         |
|--------------------------------|-------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------|
| `--i`                          | `-i`                          | Ignore failures                                                                                                                                    |                                                                                                                                               |
| `--log <path>`                 | `-log`                        | Write logs to a URI                                                                                                                                | Logs can be written to any URI with a supported scheme on the classpath.                                                                      |
| `--dryrun`                     | N/A                           | Perform a trial run with no changes made                                                                                                           |                                                                                                                                               |
| `--verbose`                    | `-v`                          | Run in verbose mode                                                                                                                                | Does not affect logfile output                                                                                                                |
| `--overwrite`                  | `-overwrite`                  | Overwrite destination                                                                                                                              | Changes how destination paths are generated identically to how Hadoop DistCP does.                                                            |
| `--update`                     | `-update`                     | Overwrite if source and destination differ in size, or checksum                                                                                    | Does not currently compare blocksize unlike Hadoop DistCP. Changes how destination paths are generated identically to how Hadoop DistCP does. |
| `--filters <path>`             | `-filters`                    | The path to a file containing a list of pattern strings, one string per line, such that paths matching the pattern will be excluded from the copy. | File can be stored on any URI with a supported scheme on the classpath.                                                                       |
| `--delete`                     | `-delete`                     | Delete the files existing in the dst but not in src                                                                                                |                                                                                                                                               |
| `--numListstatusThreads <int>` | `-numListstatusThreads`       | Number of threads to use for building file listing                                                                                                 |                                                                                                                                               |
| `--consistentPathBehaviour`    | N/A                           | Revert the path behaviour when using overwrite or update to the path behaviour of non-overwrite/non-update                                         |                                                                                                                                               |
| `--maxFilesPerTask <int>`      | N/A                           | Maximum number of files to copy in a single Spark task                                                                                             |                                                                                                                                               |
| `--maxBytesPerTask <bytes>`    | N/A                           | Maximum number of bytes to copy in a single Spark task                                                                                             |                                                                                                                                               |

### Path Behaviour

SparkDistCP aims to have the same _interesting_ path behaviour to that of Hadoop DistCP (specifically around update and overwrite).

## What is currently missing from SparkDistCP?

SparkDistCP is not a complete like-for-like reimplementation of Hadoop DistCP and there are differences in behaviour and features:

* No use of blocks, including during the copy and for comparison when using the `update` flag
* No use of snapshots
* No atomic commit option
* No preserve flag
* No append flag
* No file list flag
* No option to limit bandwidth
* No option to skip CRC check
* When using the delete option files are **not** moved into trash
* The log file in no way resembles that created by Hadoop DistCP

## How can I contribute to SparkDistCP?

We welcome all users to contribute to the development of SparkDistCP by raising pull-requests. We kindly ask that you include suitable unit tests along with proposed changes.

As you can see above, there is a wealth of work that can be done on SparkDistCP to reach feature parity with hadoop DistCP.

## What is SparkDistCP licensed under?

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Copyright 2019 Cox Automotive UK Limited
