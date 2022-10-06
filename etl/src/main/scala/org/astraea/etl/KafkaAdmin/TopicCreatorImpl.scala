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

import scala.collection.mutable.ListBuffer
import scala.collection.JavaConverters._
import scala.collection.convert.ImplicitConversions.`list asScalaBuffer`

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

  override def create(): Unit = {
    if (admin.topicNames(false).toCompletableFuture.get().contains(topic)) {
      val topicPartitions =
        admin.topicPartitions(ListBuffer(topic).toSet.asJava).toCompletableFuture.get()
      if (!topicPartitions.size().equals(numberOfPartitions))
        throw new IllegalArgumentException(
          s"$topic is existent but its partitions:${topicPartitions.size()} is not equal to expected $numberOfPartitions"
        )

      admin
        .replicas(Set(topic).asJava).toCompletableFuture.get()
        .forEach(replica =>
          if (replica.size() != numberOfReplicas)
            throw new IllegalArgumentException(
              s"$topic is existent but its replicas:${replica.size()} is not equal to expected $numberOfReplicas"
            )
        )

      val actualConfigs =
        admin.topics(ListBuffer(topic).toSet.asJava).toCompletableFuture.get().head.config().raw()

      //Confirm only the incoming config
      configs.foreach(config =>
        if (
          !Option(actualConfigs.get(config._1))
            .exists(actual => actual.equals(config._2))
        ) {
          throw new IllegalArgumentException(
            s"$topic is existent but its config:<${config._1}, ${actualConfigs
              .get(config._1)}> is not equal to expected <${config._1},${config._2}>"
          )
        }
      )

      // ok, the existent topic is totally equal to what we want to create.
    } else {
      admin
        .creator()
        .topic(topic)
        .numberOfPartitions(numberOfPartitions)
        .numberOfReplicas(numberOfReplicas)
        .configs(configs.asJava)
        .run()
    }
  }
}

object TopicCreatorImpl {
  def apply(admin: AsyncAdmin): TopicCreatorImpl = {
    new TopicCreatorImpl(admin)
  }
}
