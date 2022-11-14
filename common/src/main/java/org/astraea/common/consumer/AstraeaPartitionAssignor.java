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
package org.astraea.common.consumer;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.common.Cluster;
import org.astraea.common.admin.TopicPartition;

public interface AstraeaPartitionAssignor extends ConsumerPartitionAssignor {

  /**
   * Perform the group assignment given the members' subscription and the partition load.
   *
   * @param subscriptions Map from the member id to their respective topic subscription.
   * @param topicPartitionWithLoad Map from the topic-partition to their load. The higher value is, the
   *     more load is.
   * @return Map from each member to the list of partitions assigned to them.
   */
  Map<String, List<TopicPartition>> assign(
      Map<String, Subscription> subscriptions, Map<TopicPartition, Double> topicPartitionWithLoad);

  /**
   * Compute the load of all the topic-partitions that consumer group's members consumed.
   *
   * @param topicPartitions All the partitions in all the topics which the members subscribed.
   * @param metadata The cluster metadata is used to collect Kafka brokers' host and port.
   * @return Map from each topic-partition to their load.
   */
  Map<TopicPartition, Double> getPartitionsLoad(Set<TopicPartition> topicPartitions, Cluster metadata);
}
