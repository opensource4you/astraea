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

import com.beust.jcommander.ParameterException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.kafka.common.Cluster;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.ReplicaInfo;
import org.astraea.common.admin.TopicPartition;

public interface ConsumerPartitionAssignor
    extends org.apache.kafka.clients.consumer.ConsumerPartitionAssignor {

  /**
   * Perform the group assignment given the member subscriptions and current cluster metadata.
   *
   * @param subscriptions Map from the member id to their respective topic subscription.
   * @param metadata Current topic/broker metadata known by consumer.
   * @return Map from each member to the list of partitions assigned to them.
   */
  Map<String, List<TopicPartition>> assign(
      Map<String, Subscription> subscriptions, ClusterInfo<ReplicaInfo> metadata);

  @Override
  default String name() {
    return "astraea";
  }

  @Override
  default org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.GroupAssignment assign(
      Cluster metadata,
      org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.GroupSubscription
          groupSubscription) {

    // convert Kafka's data structure to ours
    var clusterInfo = ClusterInfo.of(metadata);
    var subscriptionsPerMember = GroupSubscription.from(groupSubscription).groupSubscription();

    return new org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.GroupAssignment(
        assign(subscriptionsPerMember, clusterInfo).entrySet().stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    e ->
                        new org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.Assignment(
                            e.getValue().stream()
                                .map(TopicPartition::to)
                                .collect(Collectors.toUnmodifiableList())))));
  }

  final class Subscription {
    private final List<String> topics;
    private final Map<String, String> userData;
    private final List<TopicPartition> ownedPartitions;
    private Optional<String> groupInstanceId;

    public Subscription(
        List<String> topics, Map<String, String> userData, List<TopicPartition> ownedPartitions) {
      this.topics = topics;
      this.userData = userData;
      this.ownedPartitions = ownedPartitions;
      this.groupInstanceId = Optional.empty();
    }

    public Subscription(List<String> topics, List<TopicPartition> ownedPartitions) {
      this.topics = topics;
      this.ownedPartitions = ownedPartitions;
      this.userData = null;
      this.groupInstanceId = Optional.empty();
    }

    public List<String> topics() {
      return topics;
    }

    public Map<String, String> userData() {
      return userData;
    }

    public List<TopicPartition> ownedPartitions() {
      return ownedPartitions;
    }

    public void setGroupInstanceId(Optional<String> groupInstanceId) {
      this.groupInstanceId = groupInstanceId;
    }

    public Optional<String> groupInstanceId() {
      return groupInstanceId;
    }

    public static Subscription from(
        org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.Subscription subscription) {
      Subscription ourSubscription;
      // convert astraea topic-partition into Kafka topic-partition
      var ownPartitions =
          subscription.ownedPartitions() == null
              ? null
              : subscription.ownedPartitions().stream()
                  .map(TopicPartition::from)
                  .collect(Collectors.toUnmodifiableList());

      var kafkaUserData = subscription.userData();
      // convert ByteBuffer into Map<String,String>
      if (kafkaUserData != null) {
        var ourUserData = convert(StandardCharsets.UTF_8.decode(kafkaUserData).toString());
        ourSubscription = new Subscription(subscription.topics(), ourUserData, ownPartitions);
      } else ourSubscription = new Subscription(subscription.topics(), ownPartitions);

      // check groupInstanceId if it's empty or not
      if (!subscription.groupInstanceId().equals(Optional.empty()))
        ourSubscription.setGroupInstanceId(subscription.groupInstanceId());

      // return astraea Subscription
      return ourSubscription;
    }

    private static Map<String, String> convert(String value) {
      return Arrays.stream(value.split(","))
          .map(
              item -> {
                var keyValue = item.split("=");
                if (keyValue.length != 2)
                  throw new ParameterException("incorrect userData format: " + item);
                return Map.entry(keyValue[0], keyValue[1]);
              })
          .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }
  }

  final class Assignment {
    private List<TopicPartition> partitions;
    private Map<String, String> userData;

    public Assignment(List<TopicPartition> partitions, Map<String, String> userData) {
      this.partitions = partitions;
      this.userData = userData;
    }

    public Assignment(List<TopicPartition> partitions) {
      this(partitions, null);
    }

    public List<TopicPartition> partitions() {
      return partitions;
    }

    public Map<String, String> userData() {
      return userData;
    }

    @Override
    public String toString() {
      return "Assignment("
          + "partitions="
          + partitions
          + (userData == null ? "" : ", user data= " + userData)
          + ')';
    }
  }

  final class GroupSubscription {
    private final Map<String, Subscription> subscriptions;

    public GroupSubscription(Map<String, Subscription> subscriptions) {
      this.subscriptions = subscriptions;
    }

    public Map<String, Subscription> groupSubscription() {
      return subscriptions;
    }

    public static GroupSubscription from(
        org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.GroupSubscription
            groupSubscription) {
      return new GroupSubscription(
          groupSubscription.groupSubscription().entrySet().stream()
              .collect(Collectors.toMap(Map.Entry::getKey, e -> Subscription.from(e.getValue()))));
    }

    @Override
    public String toString() {
      return "GroupSubscription(" + "subscriptions=" + subscriptions + ")";
    }
  }

  final class GroupAssignment {
    private final Map<String, Assignment> assignments;

    public GroupAssignment(Map<String, Assignment> assignments) {
      this.assignments = assignments;
    }

    public Map<String, Assignment> groupAssignment() {
      return assignments;
    }

    @Override
    public String toString() {
      return "GroupAssignment(" + "assignments=" + assignments + ")";
    }
  }
}
