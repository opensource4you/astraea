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

import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.streaming.{DataStreamWriter, OutputMode}
import org.astraea.common.admin.AsyncAdmin

import scala.concurrent.Future
import scala.collection.JavaConverters._

object KafkaWriter {
  def createTopic(
      admin: AsyncAdmin,
      metadata: Metadata
  ): Future[java.lang.Boolean] = {
    Utils.asScala(
      admin
        .creator()
        .topic(metadata.topicName)
        .numberOfPartitions(metadata.numPartitions)
        .numberOfReplicas(metadata.numReplicas)
        .configs(metadata.topicConfig.asJava)
        .run()
    )
  }

  def writeToKafka(
      df: DataFrame,
      metaData: Metadata
  ): DataStreamWriter[Row] = {
    df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .writeStream
      .outputMode(OutputMode.Append())
      .format("kafka")
      .option(
        "kafka." +
          ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
        metaData.kafkaBootstrapServers
      )
      .option(
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringSerializer"
      )
      .option(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringSerializer"
      )
      .option("topic", metaData.topicName)
      .option(ProducerConfig.ACKS_CONFIG, "all")
      .option(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
      .option("checkpointLocation", metaData.sinkPath + "/checkpoint")
  }
}
