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

import java.io.File
import java.util.Properties
import scala.collection.JavaConverters._

/** Parameters required for Astraea ETL.
  *
  * @param sourcePath
  *   The data source path should be a directory.
  * @param sinkPath
  *   The data sink path should be a directory.
  * @param column
  *   The CSV Column Name.For example:stringA,stringB,stringC...
  * @param primaryKeys
  *   Primary keys.
  * @param kafkaBootstrapServers
  *   The Kafka bootstrap servers.
  * @param topicName
  *   Set your topic name, if it is empty that will set it to "'spark'-'current
  *   time by hourDay'-'random number'".
  * @param numPartitions
  *   Set the number of topic partitions, if it is empty that will set it to 15.
  * @param numReplicas
  *   Set the number of topic replicas, if it is empty that will set it to 3.
  * @param topicConfig
  *   The rest of the topic can be configured parameters.For example:
  *   keyA:valueA,keyB:valueB,keyC:valueC...
  * @param deploymentModel
  *   Set deployment model, which will be used in
  *   SparkSession.builder().master(deployment.model).Two settings are currently
  *   supported spark://HOST:PORT and local[*].
  */
class Metadata private (
    var deploymentModel: String,
    var sourcePath: File,
    var sinkPath: File,
    var column: Map[String, DataType],
    var primaryKeys: Map[String, DataType],
    var kafkaBootstrapServers: String,
    var topicName: String,
    var numPartitions: Int,
    var numReplicas: Short,
    var topicConfig: Map[String, String]
) {
  protected def this() = this(
    "deploymentModel",
    new File(""),
    new File(""),
    Map.empty,
    Map.empty,
    "kafkaBootstrapServers",
    "topicName",
    -1,
    -1,
    Map.empty
  )

  protected def this(pb: Metadata) = this(
    pb.deploymentModel,
    pb.sourcePath,
    pb.sinkPath,
    pb.column,
    pb.primaryKeys,
    pb.kafkaBootstrapServers,
    pb.topicName,
    pb.numPartitions,
    pb.numReplicas,
    pb.topicConfig
  )

  def deploymentMode(str: String): Metadata = {
    this.deploymentModel = str
    new Metadata(this)
  }

  def sourcePath(file: File): Metadata = {
    this.sourcePath = file
    new Metadata(this)
  }

  def sinkPath(file: File): Metadata = {
    this.sinkPath = file
    new Metadata(this)
  }

  def columns(map: Map[String, DataType]): Metadata = {
    this.column = map
    new Metadata(this)
  }

  def primaryKey(map: Map[String, DataType]): Metadata = {
    this.primaryKeys = map
    new Metadata(this)
  }

  def kafkaBootstrapServers(str: String): Metadata = {
    this.kafkaBootstrapServers = str
    new Metadata(this)
  }

  def topicName(str: String): Metadata = {
    this.topicName = str
    new Metadata(this)
  }

  def numPartitions(num: Int): Metadata = {
    this.numPartitions = num
    new Metadata(this)
  }

  def numReplicas(num: Short): Metadata = {
    this.numReplicas = num
    new Metadata(this)
  }

  def topicConfig(map: Map[String, String]): Metadata = {
    this.topicConfig = map
    new Metadata(this)
  }
}

object Metadata {
  private[this] val SOURCE_PATH = "source.path"
  private[this] val SINK_PATH = "sink.path"
  private[this] val COLUMN_NAME = "column.name"
  private[this] val PRIMARY_KEYS = "primary.keys"
  private[this] val KAFKA_BOOTSTRAP_SERVERS = "kafka.bootstrap.servers"
  private[this] val TOPIC_NAME = "topic.name"
  private[this] val TOPIC_PARTITIONS = "topic.partitions"
  private[this] val TOPIC_REPLICAS = "topic.replicas"
  private[this] val TOPIC_CONFIG = "topic.config"
  private[this] val DEPLOYMENT_MODEL = "deployment.model"

  private[this] val DEFAULT_PARTITIONS = "15"
  private[this] val DEFAULT_REPLICAS = "1"

  def of(): Metadata = {
    new Metadata()
  }

  //Parameters needed to configure ETL.
  def apply(path: File): Metadata = {
    val properties = readProp(path).asScala

    var metadata = Metadata.of()
    properties.foreach(entry =>
      entry._1 match {
        case DEPLOYMENT_MODEL =>
          metadata = metadata.deploymentMode(DeployModel.process(entry._2))
        case SOURCE_PATH =>
          metadata = metadata.sourcePath(SourcePath.process(entry._2))
        case SINK_PATH =>
          metadata = metadata.sinkPath(SinkPath.process(entry._2))
        case COLUMN_NAME =>
          metadata = metadata.columns(ColumnName.process(entry._2))
        case PRIMARY_KEYS =>
          metadata = metadata.primaryKey(PrimaryKeys.process(entry._2))
        case KAFKA_BOOTSTRAP_SERVERS =>
          metadata = metadata.kafkaBootstrapServers(
            KafkaBootstrapServers.process(entry._2)
          )
        case TOPIC_NAME =>
          metadata = metadata.topicName(TopicName.process(entry._2))
        case TOPIC_PARTITIONS =>
          metadata = metadata.numPartitions(NumPartitions.process(entry._2))
        case TOPIC_REPLICAS =>
          metadata = metadata.numReplicas(NumReplicas.process(entry._2))
        case TOPIC_CONFIG =>
          metadata = metadata.topicConfig(TopicConfig.process(entry._2))
      }
    )
    metadata
  }

  //Handling the topic.parameters parameter.
  def requirePair(tConfig: String): Map[String, String] = {
    Option(tConfig)
      .map(
        _.split(",")
          .map(_.split("="))
          .map { elem =>
            if (elem.length != 2) {
              throw new IllegalArgumentException(
                s"The ${elem.mkString(",")} format of topic parameters is wrong.For example: keyA=valueA,keyB=valueB,keyC=valueC..."
              )
            }
            (elem(0), elem(1))
          }
          .toMap
      )
      .getOrElse(Map.empty[String, String])
  }

  private[this] def readProp(path: File): Properties = {
    val properties = new Properties()
    Utils.Using(scala.io.Source.fromFile(path)) { bufferedSource =>
      properties.load(bufferedSource.reader())
    }
    properties
  }

  /** Store information related to ETL properties metadata.
    *
    * @param metaType
    *   MetaData name
    * @param require
    *   Is this a required field
    * @param default
    *   When not required, return default if the user does not fill in the
    *   fields.
    */
  sealed abstract class MetaDataType(
      metaType: String,
      require: Boolean,
      default: String
  ) {
    def value: String = {
      metaType
    }

    def require(): Boolean = {
      require
    }

    def default(): String = {
      default
    }

    def parseEmptyStr(str: String): String = {
      var ans = str
      if (str.isEmpty) {
        if (require)
          throw new NullPointerException(
            s"$value}+ is null.You must configure $metaType."
          )
        else
          ans = default()
      }
      ans
    }
  }

  case object SourcePath extends MetaDataType(SOURCE_PATH, true, "") {
    def process(str: String): File = {
      Utils.requireFolder(
        parseEmptyStr(str)
      )
    }
  }

  case object SinkPath extends MetaDataType(SINK_PATH, true, "") {
    def process(str: String): File = {
      Utils.requireFolder(
        parseEmptyStr(str)
      )
    }
  }

  case object ColumnName extends MetaDataType(COLUMN_NAME, true, "") {
    def process(str: String): Map[String, DataType] = {
      columnParse(COLUMN_NAME, parseEmptyStr(str))
    }
  }

  case object PrimaryKeys extends MetaDataType(PRIMARY_KEYS, true, "") {
    def process(str: String): Map[String, DataType] = {
      primaryKeyParse(
        parseEmptyStr(str),
        columnParse(COLUMN_NAME, ColumnName.parseEmptyStr(str))
      )
    }
  }

  case object KafkaBootstrapServers
      extends MetaDataType(KAFKA_BOOTSTRAP_SERVERS, true, "") {
    def process(str: String): String = {
      parseEmptyStr(str)
    }
  }

  case object TopicName extends MetaDataType(TOPIC_NAME, true, "") {
    def process(str: String): String = {
      parseEmptyStr(str)
    }
  }

  case object DeployModel extends MetaDataType(DEPLOYMENT_MODEL, true, "") {
    def process(str: String): String = {
      requireDeployMode(DEPLOYMENT_MODEL, parseEmptyStr(str))
    }
  }

  case object NumPartitions
      extends MetaDataType(TOPIC_PARTITIONS, false, DEFAULT_PARTITIONS) {
    def process(str: String): Integer = {
      parseEmptyStr(str).toInt
    }
  }

  case object NumReplicas
      extends MetaDataType(TOPIC_REPLICAS, false, DEFAULT_REPLICAS) {
    def process(str: String): Short = {
      parseEmptyStr(str).toShort
    }
  }

  case object TopicConfig extends MetaDataType(TOPIC_CONFIG, false, null) {
    def process(str: String): Map[String, String] = {
      requirePair(parseEmptyStr(str))
    }
  }

  def of(meta: String): MetaDataType = {
    val value = all.filter(tp => tp.value != meta)
    if (value.isEmpty) {
      throw new IllegalArgumentException(
        s"$meta is not supported Metadata type in properties.The data types supported ${all.mkString(",")}."
      )
    }
    value.head
  }

  /** @return
    *   All supported metaData types.
    */
  def all: Seq[MetaDataType] = {
    Seq(
      DeployModel,
      SourcePath,
      SinkPath,
      ColumnName,
      PrimaryKeys,
      KafkaBootstrapServers,
      TopicName,
      NumPartitions,
      NumReplicas,
      TopicConfig
    )
  }
  def primaryKeyParse(
      prop: String,
      columnName: Map[String, DataType]
  ): Map[String, DataType] = {
    val cols = columnName.keys.toArray
    val primaryKeys = requireNonidentical(PRIMARY_KEYS, prop)
    val combine = primaryKeys.keys.toArray ++ cols
    if (combine.distinct.length != columnName.size) {
      val column = columnName.keys.toArray
      throw new IllegalArgumentException(
        s"The ${combine
          .diff(column)
          .mkString(
            PRIMARY_KEYS + "(",
            ", ",
            ")"
          )} not in column. All $PRIMARY_KEYS should be included in the column."
      )
    }
    DataType.of(primaryKeys)
  }

  def columnParse(
      key: String,
      prop: String
  ): Map[String, DataType] = {
    requireNonidentical(key, prop)
    Option(prop)
      .map(
        _.split(",")
          .map(_.split("="))
          .map { elem =>
            (elem(0), DataType.of(elem(1)))
          }
          .toMap
      )
      .get
  }

  //No duplicate values should be set.
  def requireNonidentical(
      string: String,
      prop: String
  ): Map[String, String] = {
    val array = prop.split(",")
    val map = requirePair(prop)
    if (map.size != array.length) {
      val column = map.keys.toArray
      throw new IllegalArgumentException(
        s"${array
          .diff(column)
          .mkString(
            string + " (",
            ", ",
            ")"
          )} is duplication. The $string should not be duplicated."
      )
    }
    map
  }

  //spark://host:port or local[*]
  def requireDeployMode(str: String, prop: String): String = {
    if (!DeployMode.deployMatch(prop)) {
      throw new IllegalArgumentException(
        s"$prop not a supported deployment model. Please check $str."
      )
    }
    prop
  }
}
