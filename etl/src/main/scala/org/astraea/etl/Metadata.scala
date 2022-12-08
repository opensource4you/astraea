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

import org.astraea.etl.DataColumn.columnParse

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
  *   The CSV Column metadata.Contains column name, data type and is pk or not.
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
  * @param deployModel
  *   Set deployment model, which will be used in
  *   SparkSession.builder().master(deployment.model).Two settings are currently
  *   supported spark://HOST:PORT and local[*].
  */
case class Metadata private (
    var deployModel: String,
    var sourcePath: File,
    var sinkPath: File,
    var column: Seq[DataColumn],
    var kafkaBootstrapServers: String,
    var topicName: String,
    var numPartitions: Int,
    var numReplicas: Short,
    var topicConfig: Map[String, String],
    var checkpoint: File
)

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
  private[this] val DEPLOY_MODEL = "deploy.model"
  private[this] val CHECKPOINT = "checkpoint"

  private[this] val DEFAULT_PARTITIONS = "15"
  private[this] val DEFAULT_REPLICAS = "1"

  def builder(): MetadataBuilder = {
    MetadataBuilder.of()
  }

  // Parameters needed to configure ETL.
  def apply(path: File): Metadata = {
    val properties = readProp(path).asScala

    var metadataBuilder = Metadata.builder()
    properties.foreach(entry =>
      entry._1 match {
        case DEPLOY_MODEL =>
          metadataBuilder =
            metadataBuilder.deploymentMode(DeployModel.process(entry._2))
        case SOURCE_PATH =>
          metadataBuilder =
            metadataBuilder.sourcePath(SourcePath.process(entry._2))
        case SINK_PATH =>
          metadataBuilder = metadataBuilder.sinkPath(SinkPath.process(entry._2))
        case COLUMN_NAME =>
          metadataBuilder = metadataBuilder.columns(
            ColumnName.process(entry._2, properties(PRIMARY_KEYS))
          )
        case KAFKA_BOOTSTRAP_SERVERS =>
          metadataBuilder = metadataBuilder.kafkaBootstrapServers(
            KafkaBootstrapServers.process(entry._2)
          )
        case TOPIC_NAME =>
          metadataBuilder =
            metadataBuilder.topicName(TopicName.process(entry._2))
        case TOPIC_PARTITIONS =>
          metadataBuilder =
            metadataBuilder.numPartitions(NumPartitions.process(entry._2))
        case TOPIC_REPLICAS =>
          metadataBuilder =
            metadataBuilder.numReplicas(NumReplicas.process(entry._2))
        case TOPIC_CONFIG =>
          metadataBuilder =
            metadataBuilder.topicConfig(TopicConfig.process(entry._2))
        case CHECKPOINT =>
          metadataBuilder =
            metadataBuilder.checkpoint(Checkpoint.process(entry._2))
        case _ =>
      }
    )
    metadataBuilder.build()
  }

  // Handling the topic.parameters parameter.
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

  case object Checkpoint extends MetaDataType(CHECKPOINT, true, "") {
    def process(str: String): File = {
      Utils.requireFolder(
        parseEmptyStr(str)
      )
    }
  }

  case object ColumnName extends MetaDataType(COLUMN_NAME, true, "") {
    def process(cols: String, pk: String): Seq[DataColumn] = {
      columnParse(parseEmptyStr(cols), parseEmptyStr(pk))
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

  case object DeployModel extends MetaDataType(DEPLOY_MODEL, true, "") {
    def process(str: String): String = {
      requireDeployMode(DEPLOY_MODEL, parseEmptyStr(str))
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
      KafkaBootstrapServers,
      TopicName,
      NumPartitions,
      NumReplicas,
      TopicConfig
    )
  }

  // spark://host:port or local[*]
  def requireDeployMode(str: String, prop: String): String = {
    if (!DeployMode.deployMatch(prop)) {
      throw new IllegalArgumentException(
        s"$prop not a supported deployment model. Please check $str."
      )
    }
    prop
  }

  class MetadataBuilder private (
      private var deploymentModel: String,
      private var sourcePath: File,
      private var sinkPath: File,
      private var columns: Seq[DataColumn],
      private var kafkaBootstrapServers: String,
      private var topicName: String,
      private var numPartitions: Int,
      private var numReplicas: Short,
      private var topicConfig: Map[String, String],
      private var checkpoint: File
  ) {
    protected def this() = this(
      "deploymentModel",
      new File(""),
      new File(""),
      Seq.empty,
      "kafkaBootstrapServers",
      "topicName",
      -1,
      -1,
      Map.empty,
      new File("")
    )

    def deploymentMode(str: String): MetadataBuilder = {
      this.deploymentModel = str
      this
    }

    def sourcePath(file: File): MetadataBuilder = {
      this.sourcePath = file
      this
    }

    def sinkPath(file: File): MetadataBuilder = {
      this.sinkPath = file
      this
    }

    def columns(map: Seq[DataColumn]): MetadataBuilder = {
      this.columns = map
      this
    }

    def kafkaBootstrapServers(str: String): MetadataBuilder = {
      this.kafkaBootstrapServers = str
      this
    }

    def topicName(str: String): MetadataBuilder = {
      this.topicName = str
      this
    }

    def numPartitions(num: Int): MetadataBuilder = {
      this.numPartitions = num
      this
    }

    def numReplicas(num: Short): MetadataBuilder = {
      this.numReplicas = num
      this
    }

    def topicConfig(map: Map[String, String]): MetadataBuilder = {
      this.topicConfig = map
      this
    }

    def checkpoint(file: File): MetadataBuilder = {
      this.checkpoint = file
      this
    }

    def build(): Metadata = {
      Metadata(
        this.deploymentModel,
        this.sourcePath,
        this.sinkPath,
        this.columns,
        this.kafkaBootstrapServers,
        this.topicName,
        this.numPartitions,
        this.numReplicas,
        this.topicConfig,
        this.checkpoint
      )
    }
  }

  object MetadataBuilder {
    private[Metadata] def of(): MetadataBuilder = {
      new MetadataBuilder()
    }
  }
}
