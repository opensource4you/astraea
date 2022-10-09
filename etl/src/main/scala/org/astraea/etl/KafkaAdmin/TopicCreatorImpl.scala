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
package org.astraea.etl.KafkaAdmin

import org.astraea.common.admin.AsyncAdmin

import java.util.concurrent.{CompletableFuture, CompletionStage}
import scala.collection.mutable.ListBuffer
import scala.collection.JavaConverters._
import scala.collection.convert.ImplicitConversions.`list asScalaBuffer`
import scala.concurrent.Future

class TopicCreatorImpl(admin: AsyncAdmin) extends TopicCreator {
  private[this] var topic: String = ""
  private[this] var numberOfPartitions: Int = 1
  private[this] var numberOfReplicas: Short = 1
  private[this] var configs: Map[String, String] = Map.empty[String, String]

  override def topic(name: String): TopicCreator = {
    this.topic = name
    this
  }

  override def numberOfPartitions(num: Int): TopicCreator = {
    this.numberOfPartitions = num
    this
  }

  override def numberOfReplicas(num: Short): TopicCreator = {
    this.numberOfReplicas = num
    this
  }

  override def config(key: String, value: String): TopicCreator = {
    this.configs += (key -> value)
    this
  }

  override def config(map: Map[String, String]): TopicCreator = {
    this.configs = this.configs ++ map
    this
  }

  //Starting Scala 2.13, the standard library includes scala.jdk.FutureConverters which provides Java to Scala CompletableFuture/Future implicit conversions
  override def create(): CompletableFuture[java.lang.Boolean] = {
    admin
      .creator()
      .topic(topic)
      .numberOfPartitions(numberOfPartitions)
      .numberOfReplicas(numberOfReplicas)
      .configs(configs.asJava)
      .run()
      .toCompletableFuture
  }
}

object TopicCreatorImpl {
  def apply(admin: AsyncAdmin): TopicCreatorImpl = {
    new TopicCreatorImpl(admin)
  }
}
