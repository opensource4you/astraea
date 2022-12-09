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

import org.astraea.common.admin.Admin
import org.astraea.etl.Reader.createSchema
import org.astraea.etl.Utils.createTopic
import scopt.{OParser, OParserBuilder}

import java.util.concurrent.TimeUnit
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.sys.exit

object Spark2Kafka {
  def executor(args: Config, duration: Int): Unit = {
    val metaData = Metadata(Utils.requireFile(args.properties))
    Utils.Using(Admin.of(metaData.kafkaBootstrapServers)) { admin =>
      val pk = metaData.column.filter(col => col.isPK).map(col => col.name)
      Await.result(createTopic(admin, metaData), Duration.Inf)
      val df = Reader
        .of()
        .spark(metaData.deployModel)
        .schema(
          createSchema(
            metaData.column.map(col => (col.name, col.dataType)).toMap
          )
        )
        .sinkPath(metaData.sinkPath.getPath)
        .primaryKeys(pk)
        .readCSV(metaData.sourcePath.getPath, args.allowBlankLine)
        .csvToJSON(pk)

      val query = Writer
        .of()
        .dataFrameOp(df)
        .target(metaData.topicName)
        .checkpoint(metaData.checkpoint.toString)
        .writeToKafka(metaData.kafkaBootstrapServers)
        .start()
      if (duration > 0) {
        query.awaitTermination(Duration(duration, TimeUnit.SECONDS).toMillis)
      } else {
        query.awaitTermination()
      }
    }
  }

  case class Config(properties: String = "", allowBlankLine: Boolean = true)
  val builder: OParserBuilder[Config] = OParser.builder[Config]

  val argParser: OParser[Unit, Config] = {
    import builder._
    OParser.sequence(
      programName("myprog"),
      head("myprog", "0.1"),
      opt[String]("properties")
        .action((s, c) => c.copy(properties = s))
        .text("The properties file of astraea etl."),
      opt[Boolean]("allowBlankLine")
        .action((d, c) => c.copy(allowBlankLine = d))
        .text("Allow blank lines when processing csv files."),
      checkConfig(c => {
        if (!c.properties.isBlank) {
          success
        } else {
          failure("You must configure properties path.")
        }
      })
    )

  }

  def main(args: Array[String]): Unit = {
    OParser.parse(argParser, args, Config()) match {
      case Some(config) =>
        executor(config, 0)
      case _ =>
        exit(1)
    }
  }
}
