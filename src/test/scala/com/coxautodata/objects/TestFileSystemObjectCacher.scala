package com.coxautodata.objects

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class TestFileSystemObjectCacher extends AnyFunSpec with Matchers {

  it("should create and cache a filesystem") {

    val conf = new Configuration()

    val cache = new FileSystemObjectCacher(conf)

    cache.get(new URI("file:///test/file")) should be(None)

    cache.getOrCreate(new URI("file:///test/file")).getUri.toString should be("file:///")

    cache.get(new URI("file:///test2/file2")).map(_.getUri.toString) should be(Some("file:///"))

  }

}
