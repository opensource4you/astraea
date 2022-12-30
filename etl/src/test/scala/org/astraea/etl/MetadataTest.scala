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

import org.junit.jupiter.api.{Assertions, Test}

import java.io.{File, FileOutputStream}
import java.nio.file.Files
import java.util.Properties
import scala.util.Using

class MetadataTest {

  @Test
  def testTopicConfigs(): Unit = {
    val path = save(
      Map(
        Metadata.SOURCE_PATH_KEY -> "/tmp/aa",
        Metadata.TOPIC_NAME_KEY -> "bb",
        Metadata.COLUMN_NAME_KEY -> "a,b",
        Metadata.KAFKA_BOOTSTRAP_SERVERS_KEY -> "host:1122",
        Metadata.TOPIC_CONFIGS_KEY -> "a=b,c=d"
      )
    )
    val metadata = Metadata.of(path)
    Assertions.assertEquals(2, metadata.topicConfigs.size)
    Assertions.assertEquals("b", metadata.topicConfigs("a"))
    Assertions.assertEquals("d", metadata.topicConfigs("c"))
  }

  @Test
  def testDefault(): Unit = {
    val path = save(
      Map(
        Metadata.SOURCE_PATH_KEY -> "/tmp/aa",
        Metadata.TOPIC_NAME_KEY -> "bb",
        Metadata.COLUMN_NAME_KEY -> "a,b",
        Metadata.KAFKA_BOOTSTRAP_SERVERS_KEY -> "host:1122"
      )
    )
    val metadata = Metadata.of(path)

    Assertions.assertEquals("/tmp/aa", metadata.sourcePath)
    Assertions.assertEquals("host:1122", metadata.kafkaBootstrapServers)
    Assertions.assertEquals("bb", metadata.topicName)
    Assertions.assertEquals(2, metadata.columns.size)
    Assertions.assertEquals("a", metadata.columns.head.name)
    Assertions.assertTrue(metadata.columns.head.isPk)
    Assertions.assertEquals(DataType.StringType, metadata.columns.head.dataType)
    Assertions.assertEquals("b", metadata.columns(1).name)
    Assertions.assertTrue(metadata.columns(1).isPk)
    Assertions.assertEquals(DataType.StringType, metadata.columns(1).dataType)
  }

  private def save(props: Map[String, String]): File = {
    val prop = new Properties
    props.foreach { case (k, v) =>
      prop.put(k, v)
    }
    val file = Files.createTempFile("test", "props").toFile
    Using(new FileOutputStream(file)) { output =>
      prop.store(output, null)
    }
    file
  }
}
