/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.astraea.etl

import org.astraea.etl.Configuration.{
  primaryKeys,
  requireNonidentical,
  topicParameters
}
import org.junit.jupiter.api.{BeforeEach, Test}
import org.scalatest.Assertions.assertThrows

import java.io.{File, FileOutputStream}
import java.nio.file.Files.createTempFile
import java.util.Properties
import scala.util.{Try, Using}

class ProcessPropertiesTest {
  var file = new File("")
  var path = ""

  @BeforeEach def setup(): Unit = {
    file = createTempFile("local_kafka", ".properties").toFile
    path = file.getAbsolutePath
    testConfig()
  }

  @Test def defaultTest(): Unit = {
    val config = Configuration(Utils.requireFile(file.getAbsolutePath))
    assert(config.sourcePath.equals(new File(file.getParent)))
    assert(config.sinkPath.equals(new File(file.getParent)))
    assert(config.columnName sameElements Array[String]("ID", "KA", "KB", "KC"))
    assert(config.primaryKeys sameElements Array[String]("ID"))
    assert(config.kafkaBootstrapServers.equals("0.0.0.0"))
    assert(config.numPartitions.equals(15))
    assert(config.numReplicas.equals(1))
    assert(config.topicName.nonEmpty)
    assert(config.topicConfig.isEmpty)
  }

  @Test def configuredTest(): Unit = {
    val prop = new Properties
    Using(scala.io.Source.fromFile(file)) { bufferedSource =>
      prop.load(bufferedSource.reader())
    }
    prop.setProperty("topic.partitions", "30")
    prop.setProperty("topic.replicas", "3")
    prop.setProperty("topic.config", "KA:VA,KB:VB")
    prop.store(new FileOutputStream(file), null)

    val config = Configuration(Utils.requireFile(file.getAbsolutePath))
    assert(config.sourcePath.equals(new File(file.getParent)))
    assert(config.sinkPath.equals(new File(file.getParent)))
    assert(config.columnName sameElements Array[String]("ID", "KA", "KB", "KC"))
    assert(config.primaryKeys sameElements Array[String]("ID"))
    assert(config.kafkaBootstrapServers.equals("0.0.0.0"))
    assert(config.numPartitions.equals(30))
    assert(config.numReplicas.equals(3))
    assert(config.topicName.equals("spark-1"))
    assert(config.topicConfig.equals(Map("KA" -> "VA", "KB" -> "VB")))
  }

  @Test def requireNonidenticalTest(): Unit = {
    val map = Map[String, String]("data" -> "ID,KA,KB,KC,ID")
    assertThrows[IllegalArgumentException] {
      requireNonidentical("data", map)
    }
  }

  @Test def primaryKeysTest(): Unit = {
    val map =
      Map[String, String]("data" -> "ID,KA,KB,KC", "primary.keys" -> "DD")
    assertThrows[IllegalArgumentException] {
      primaryKeys(map, requireNonidentical("data", map))
    }
  }

  @Test def topicParametersTest(): Unit = {
    val map = "ID:KA,PP:KB,KC"
    assertThrows[IllegalArgumentException] {
      topicParameters(map)
    }
  }

  def testConfig(): Unit = {
    Try {
      val prop = new Properties
      prop.setProperty("source.path", file.getParent)
      prop.setProperty("sink.path", file.getParent)
      prop.setProperty("column.name", "ID,KA,KB,KC")
      prop.setProperty("primary.keys", "ID")
      prop.setProperty("kafka.bootstrap.servers", "0.0.0.0")
      prop.setProperty("topic.name", "spark-1")
      prop.setProperty("topic.partitions", "")
      prop.setProperty("topic.replicas", "")
      prop.setProperty("topic.config", "")
      prop.store(new FileOutputStream(file), null)
    }
  }
}
