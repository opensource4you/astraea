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
package org.astraea.common.metrics;

import java.util.Objects;
import java.util.Optional;
import org.astraea.common.admin.BrokerTopic;
import org.astraea.common.admin.TopicPartition;
import org.astraea.common.admin.TopicPartitionReplica;

public interface HasBeanObject {
  BeanObject beanObject();

  default String metricName() {
    return Objects.requireNonNull(
        beanObject().properties().get("name"), "beanObject must have metric name");
  }

  default long createdTimestamp() {
    return beanObject().createdTimestamp();
  }

  /**
   * @return the topic index of this metric, if the metric can't be indexed by topic, a {@link
   *     Optional#empty()} will be returned.
   */
  default Optional<String> topicIndex() {
    return Optional.ofNullable(beanObject().properties().get("topic"));
  }

  /**
   * @return the partition index of this metric, if the metric can't be indexed by partition, a
   *     {@link Optional#empty()} will be returned.
   */
  default Optional<TopicPartition> partitionIndex() {
    return topicIndex()
        .flatMap(
            topic ->
                Optional.ofNullable(beanObject().properties().get("partition"))
                    .map(Integer::parseInt)
                    .map(partition -> TopicPartition.of(topic, partition)));
  }

  /**
   * @return the replica index of this metric, if the metric can't be indexed by replica, a {@link
   *     Optional#empty()} will be returned.
   */
  default Optional<TopicPartitionReplica> replicaIndex(int broker) {
    return partitionIndex()
        .map(
            topicPartition ->
                TopicPartitionReplica.of(
                    topicPartition.topic(), topicPartition.partition(), broker));
  }

  /**
   * @return the broker-topic index of this metric, if the metric can't be indexed by broker-topic,
   *     a {@link Optional#empty()} will be returned.
   */
  default Optional<BrokerTopic> brokerTopicIndex(int broker) {
    return topicIndex().map(topic -> BrokerTopic.of(broker, topic));
  }
}
