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
import org.apache.spark.sql.functions.{col, concat, concat_ws, struct, to_json}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.streaming.DataStreamWriter
import org.apache.spark.sql.types.StructType
object CSVReader {
  def createSpark(deploymentModel: String): SparkSession = {
    SparkSession
      .builder()
      .master(deploymentModel)
      .appName("Astraea ETL")
      .getOrCreate()
  }

  def createSchema(
      cols: Map[String, DataType],
      pk: Map[String, DataType]
  ): StructType = {
    var userSchema = new StructType()
    cols.foreach(col =>
      if (pk.contains(col._1))
        userSchema = userSchema.add(col._1, col._2.value, nullable = false)
      else
        userSchema = userSchema.add(col._1, col._2.value, nullable = true)
    )
    userSchema
  }

  def readCSV(
      spark: SparkSession,
      userSchema: StructType,
      sourcePath: String
  ): DataFrame = {
    spark.readStream
      .option("sep", ",")
      .schema(userSchema)
      .csv(sourcePath)
  }

  //TODO To Kafka test
  def writeKafka(
      df: DataFrame,
      metaData: Metadata
  ): DataStreamWriter[Row] = {
    df.writeStream
      .format("kafka")
      .option(
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
      //TODO check topic
      .option("topic", metaData.topicName)
      .option(ProducerConfig.ACKS_CONFIG, "all")
      .option(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
  }

  /** Turn the original DataFrame into a key-value table.Integrate all columns
    * into one value->josh. If there are multiple primary keys, key will become
    * keyA_keyB_....
    *
    * {{{
    * Seq(Person("Michael","A.K", 29), Person("Andy","B.C", 30), Person("Justin","C.L", 19))
    *
    * Person
    * |-- FirstName: string
    * |-- SecondName: string
    * |-- Age: Integer
    *
    * Key:FirstName,SecondName
    *
    * // +-----------+---------------------------------------------------+
    * // |        key|                                              value|
    * // +-----------+---------------------------------------------------+
    * // |Michael,A.K|{"FirstName":"Michael","SecondName":"A.K","Age":29}|
    * // |   Andy,B.C|{"FirstName":"Andy","SecondName":"B.C","Age":30}   |
    * // | Justin,C.L|{"FirstName":"Justin","SecondName":"C.L","Age":19} |
    * // +-----------+---------------------------------------------------+
    * }}}
    *
    * @param dataFrame
    *   any DataFrame
    * @param pk
    *   primary keys
    * @return
    *   json df
    */
  def csvToJSON(dataFrame: DataFrame, pk: Seq[String]): DataFrame = {
    dataFrame
      .withColumn("value", to_json(struct($conforms("*"))))
      .withColumn("key", concat_ws(",", pk.map(col).seq: _*))
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
  }
}
