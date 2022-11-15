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

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;
import org.apache.kafka.common.Cluster;
import org.astraea.common.admin.TopicPartition;

public interface ConsumerPartitionAssignor
    extends org.apache.kafka.clients.consumer.ConsumerPartitionAssignor {

  /**
   * Perform the group assignment given the members' subscription and the partition load.
   *
   * @param subscriptions Map from the member id to their respective topic subscription.
   * @param topicPartitionWithLoad Map from the topic-partition to their load. The higher value is,
   *     the more load is.
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
  Map<TopicPartition, Double> getPartitionsLoad(
      Set<TopicPartition> topicPartitions, Cluster metadata);

  @Override
  default String name() {
    return "astraea";
  }

  @Override
  default GroupAssignment assign(Cluster metadata, GroupSubscription groupSubscription) {
    // step 1. find all topics that members subscribe and the number of partition in the topics
    var subscriptionsPerMember = groupSubscription.groupSubscription();
    var subscribedTopics = new HashSet<String>();
    subscriptionsPerMember
        .values()
        .forEach(subscription -> subscribedTopics.addAll(subscription.topics()));

    var topicPartitions = new HashSet<TopicPartition>();
    subscribedTopics.forEach(
        topic ->
            IntStream.range(0, metadata.partitionCountForTopic(topic))
                .forEach(i -> topicPartitions.add(TopicPartition.of(topic, i))));

    // step 2. get the load of all topic-partitions
    var topicPartitionsLoad = getPartitionsLoad(topicPartitions, metadata);

    // step 3. assign topic-partition to members based on their subscription and topic-partition's
    // load
    var rawAssignmentPerMember = assign(subscriptionsPerMember, topicPartitionsLoad);

    // step 4. wrap the assignments
    var assignments = new HashMap<String, Assignment>();

    rawAssignmentPerMember.forEach(
        (member, rawAssignment) ->
            assignments.put(member, new Assignment(TopicPartition.to(rawAssignment))));

    // step 5. return the GroupAssignment for future syncGroup request.
    return new GroupAssignment(assignments);
  }
}
