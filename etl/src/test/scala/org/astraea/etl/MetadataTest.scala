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

import org.astraea.etl.DataColumn.{columnParse, requireNonidentical}
import org.astraea.etl.DataType.StringType
import org.astraea.etl.Metadata.requirePair
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows, assertTrue}
import org.junit.jupiter.api.{BeforeEach, Test}

import java.io.{File, FileOutputStream}
import java.nio.file.Files.createTempFile
import java.util.Properties
import scala.util.Try

class MetadataTest {
  var file = new File("")
  var path = ""

  @BeforeEach def setup(): Unit = {
    file = createTempFile("local_kafka", ".properties").toFile
    path = file.getAbsolutePath
    testConfig()
  }

  @Test def defaultTest(): Unit = {
    val config = Metadata(Utils.requireFile(file.getAbsolutePath))
    assertEquals(config.sourcePath, new File(file.getParent))
    assertEquals(config.sinkPath, new File(file.getParent))
    assertEquals(
      config.column.map(col => (col.name, col.dataType)).toMap,
      Map(
        "ID" -> StringType,
        "KA" -> StringType,
        "KB" -> StringType,
        "KC" -> StringType
      )
    )
    assertEquals(
      config.column
        .filter(col => col.isPK)
        .map(col => (col.name, col.dataType))
        .toMap,
      Map("ID" -> StringType)
    )
    assertEquals(config.kafkaBootstrapServers, "0.0.0.0")
    assertEquals(config.numPartitions, 15)
    assertEquals(config.numReplicas, 1.toShort)
    assertTrue(config.topicName.nonEmpty)
    assertTrue(config.topicConfig.isEmpty)
  }

  @Test def configuredTest(): Unit = {
    val prop = new Properties
    Utils.Using(scala.io.Source.fromFile(file)) { bufferedSource =>
      prop.load(bufferedSource.reader())
    }
    prop.setProperty("topic.partitions", "30")
    prop.setProperty("topic.replicas", "3")
    prop.setProperty("topic.config", "KA=VA,KB=VB")
    prop.store(new FileOutputStream(file), null)

    val config = Metadata(Utils.requireFile(file.getAbsolutePath))
    assertTrue(config.sourcePath.equals(new File(file.getParent)))
    assertTrue(config.sinkPath.equals(new File(file.getParent)))
    assertEquals(
      config.column.map(col => (col.name, col.dataType)).toMap,
      Map(
        "ID" -> StringType,
        "KA" -> StringType,
        "KB" -> StringType,
        "KC" -> StringType
      )
    )
    assertEquals(
      config.column
        .filter(col => col.isPK)
        .map(col => (col.name, col.dataType))
        .toMap,
      Map("ID" -> StringType)
    )
    assertTrue(config.kafkaBootstrapServers.equals("0.0.0.0"))
    assertTrue(config.numPartitions.equals(30))
    assertTrue(config.numReplicas.equals(3.toShort))
    assertTrue(config.topicName.equals("spark-1"))
    assertTrue(config.topicConfig.equals(Map("KA" -> "VA", "KB" -> "VB")))
  }

  @Test def requireNonidenticalTest(): Unit = {
    assertThrows(
      classOf[IllegalArgumentException],
      () => requireNonidentical("data", "ID,KA,KB,KC,ID")
    )
  }

  @Test def primaryKeysParseTest(): Unit = {
    assertThrows(
      classOf[IllegalArgumentException],
      () =>
        columnParse("ID=string,KA=string,KB=string,KC=integer", "data=string")
    )
  }

  @Test def requirePairTest(): Unit = {
    val map = "ID=KA,PP=KB,KC"
    assertThrows(classOf[IllegalArgumentException], () => requirePair(map))
  }

  @Test def columnParseTest(): Unit = {
//    columnParse("data=string", "ID=string,KA=string,KB=string,KC=integer")
    assertThrows(
      classOf[IllegalArgumentException],
      () => columnParse("ID=string,KA=string,KB=string,KC=intege", "ID=string")
    )
  }

  @Test def typeParseTest(): Unit = {
    assertThrows(
      classOf[IllegalArgumentException],
      () => DataType.of("LLL")
    )
  }

  @Test def requireDeployModeTest(): Unit = {
    assertThrows(
      classOf[IllegalArgumentException],
      () => Metadata.requireDeployMode("deployment.model", "local")
    )
  }

  def testConfig(): Unit = {
    Try {
      val prop = new Properties
      prop.setProperty("source.path", file.getParent)
      prop.setProperty("sink.path", file.getParent)
      prop.setProperty("column.name", "ID=string,KA=string,KB=string,KC=string")
      prop.setProperty("primary.keys", "ID=string")
      prop.setProperty("kafka.bootstrap.servers", "0.0.0.0")
      prop.setProperty("topic.name", "spark-1")
      prop.setProperty("topic.partitions", "")
      prop.setProperty("topic.replicas", "")
      prop.setProperty("topic.config", "")
      prop.setProperty("deployment.model", "local[2]")
      prop.store(new FileOutputStream(file), null)
    }
  }
}
