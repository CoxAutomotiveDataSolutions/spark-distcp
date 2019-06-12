package com.coxautodata.objects

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.hadoop.conf.Configuration

/**
  * Class to make Hadoop configurations serializable; uses the
  * `Writeable` operations to do this.
  * Note: this only serializes the explicitly set values, not any set
  * in site/default or other XML resources.
  *
  */
class ConfigSerDeser(var conf: Configuration) extends Serializable {

  def this() {
    this(new Configuration())
  }

  def get(): Configuration = conf

  private def writeObject(out: java.io.ObjectOutputStream): Unit = {
    conf.write(out)
  }

  private def readObject(in: java.io.ObjectInputStream): Unit = {
    conf = new Configuration()
    conf.readFields(in)
  }

  private def readObjectNoData(): Unit = {
    conf = new Configuration()
  }
}