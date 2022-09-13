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

import org.astraea.etl.Argument.parseArgument
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}

import java.io.{File, FileOutputStream}
import java.util.Properties
import scala.util.{Try, Using}

class ProcessPropertiesTest {
  val directory = new File("")

  @BeforeEach def setup(): Unit = {
    testConfig()
  }

  @AfterEach def tearDown(): Unit = {
    val fileProp = new File("test.properties")
    if (fileProp.exists()) {
      fileProp.delete()
    }
  }

  @Test def defaultTest(): Unit = {
    val arg = parseArgument(Array("--prop.file", "test.properties"))
    val config = Configuration(arg.get.propFile)
    assert(config.sourcePath.equals(new File(directory.getAbsolutePath)))
    assert(config.sinkPath.equals(new File(directory.getAbsolutePath)))
    assert(config.columnName sameElements Array[String]("KA", "KB", "KC"))
    assert(config.primaryKeys.equals("ID"))
    assert(config.kafkaBootstrapServers.equals("0.0.0.0"))
    assert(config.numPartitions.equals(15))
    assert(config.numReplicas.equals(3))
    assert(config.topicName.nonEmpty)
    assert(config.topicParameters.isEmpty)
  }

  @Test def configuredTest(): Unit = {
    val prop = new Properties
    val file = new File("test.properties")
    Using(scala.io.Source.fromFile("test.properties")) { bufferedSource =>
      prop.load(bufferedSource.reader())
    }
    prop.setProperty("topic.name", "spark-1")
    prop.setProperty("topic.num.partitions", "30")
    prop.setProperty("topic.num.replicas", "1")
    prop.setProperty("topic.parameters", "KA:VA,KB:VB")
    prop.store(new FileOutputStream(file), null)

    val arg = parseArgument(Array("--prop.file", "test.properties"))
    val config = Configuration(arg.get.propFile)
    assert(config.sourcePath.equals(new File(directory.getAbsolutePath)))
    assert(config.sinkPath.equals(new File(directory.getAbsolutePath)))
    assert(config.columnName sameElements Array[String]("KA", "KB", "KC"))
    assert(config.primaryKeys.equals("ID"))
    assert(config.kafkaBootstrapServers.equals("0.0.0.0"))
    assert(config.numPartitions.equals(30))
    assert(config.numReplicas.equals(1))
    assert(config.topicName.equals("spark-1"))
    assert(config.topicParameters.equals(Map("KA" -> "VA", "KB" -> "VB")))
  }

  def testConfig(): Unit = {
    Try {
      val prop = new Properties
      val file = new File("test.properties")
      prop.setProperty("source.path", directory.getAbsolutePath)
      prop.setProperty("sink.path", directory.getAbsolutePath)
      prop.setProperty("column.name", "KA,KB,KC")
      prop.setProperty("primary.keys", "ID")
      prop.setProperty("kafka.bootstrap.servers", "0.0.0.0")
      prop.setProperty("topic.name", "")
      prop.setProperty("topic.num.partitions", "")
      prop.setProperty("topic.num.replicas", "")
      prop.setProperty("topic.parameters", "")
      prop.store(new FileOutputStream(file), null)
    }
  }

}
