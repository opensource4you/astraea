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

import org.astraea.common.admin.AsyncAdmin
import org.astraea.etl.Reader.createSchema
import org.astraea.etl.Utils.createTopic

import java.util.concurrent.TimeUnit
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success}

object Spark2Kafka {
  def executor(args: Array[String], duration: Duration): Unit = {
    val metaData = Metadata(Utils.requireFile(args(0)))
    Utils.Using(AsyncAdmin.of(metaData.kafkaBootstrapServers)) { admin =>
      Await.result(createTopic(admin, metaData), Duration(5, TimeUnit.SECONDS))
      val df = Reader
        .of()
        .spark(metaData.deploymentModel)
        .schema(createSchema(metaData.column))
        .sinkPath(metaData.sinkPath.getPath)
        .primaryKeys(metaData.primaryKeys.keys.toSeq)
        .readCSV(metaData.sourcePath.getPath)
        .csvToJSON(metaData.primaryKeys.keys.toSeq)

      Writer
        .of()
        .dataFrameOp(df)
        .target(metaData.topicName)
        .checkpoint(metaData.sinkPath + "/checkpoint")
        .writeToKafka(metaData.kafkaBootstrapServers)
        .start()
        .awaitTermination(duration.toMillis)
    }
  }

  def main(args: Array[String]): Unit = {
    executor(args, Duration.Inf)
  }
}
