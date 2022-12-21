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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.astraea.common.admin.Admin

import java.io.File
import scala.concurrent.duration.Duration
import scala.jdk.CollectionConverters._
import scala.util.Using

object Spark2Kafka {
  def executor(
      sparkSession: SparkSession,
      metadata: Metadata,
      duration: Duration
  ): Unit = {
    Using(Admin.of(metadata.kafkaBootstrapServers))(
      _.creator()
        .topic(metadata.topicName)
        .configs(metadata.topicConfigs.asJava)
        .numberOfPartitions(metadata.numberOfPartitions)
        .numberOfReplicas(metadata.numberOfReplicas)
        .run()
        .toCompletableFuture
        .join()
    )

    val df = ReadStreams
      .create(
        session = sparkSession,
        source = metadata.sourcePath,
        columns = metadata.columns
      )
      .csvToJSON(metadata.columns)

    Writer
      .of()
      .dataFrameOp(df)
      .target(metadata.topicName)
      .checkpoint(metadata.checkpoint)
      .writeToKafka(metadata.kafkaBootstrapServers)
      .start()
      .awaitTermination(duration.toMillis)
  }

  def main(args: Array[String]): Unit = {
    executor(
      SparkSession
        .builder()
        .appName("astraea etl")
        .getOrCreate(),
      Metadata.of(new File(args(0))),
      Duration("1000 seconds")
    )
  }
}
