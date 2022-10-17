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
package org.astraea.common.admin;

import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.astraea.common.Utils;

@Deprecated
public interface Admin extends Closeable {

  static Builder builder() {
    return new Builder();
  }

  static Admin of(String bootstrapServers) {
    return builder().bootstrapServers(bootstrapServers).build();
  }

  static Admin of(Map<String, String> configs) {
    return builder().configs(configs).build();
  }

  String clientId();

  /** @return the number of pending requests. */
  int pendingRequests();

  /**
   * @param listInternal should list internal topics or not
   * @return names of topics
   */
  Set<String> topicNames(boolean listInternal);

  /** @return names of all topics (include internal topics). */
  default Set<String> topicNames() {
    return topicNames(true);
  }

  List<Topic> topics(Set<String> names);

  /** delete topics by topic names */
  void deleteTopics(Set<String> topicNames);

  /** @return all partitions */
  default Set<TopicPartition> topicPartitions() {
    return topicPartitions(topicNames());
  }

  /**
   * @param topics target
   * @return the partitions belong to input topics
   */
  Set<TopicPartition> topicPartitions(Set<String> topics);

  /**
   * list all partitions belongs to input brokers
   *
   * @param brokerId to search
   * @return all partition belongs to brokers
   */
  Set<TopicPartition> topicPartitions(int brokerId);

  /** @return a topic creator to set all topic configs and then run the procedure. */
  TopicCreator creator();

  List<Partition> partitions(Set<String> topics);

  /** @return all consumer group ids */
  Set<String> consumerGroupIds();

  /**
   * @param consumerGroupNames consumer group names.
   * @return the member info of each consumer group
   */
  List<ConsumerGroup> consumerGroups(Set<String> consumerGroupNames);

  /** @return replica info of all partitions */
  default List<Replica> replicas() {
    return replicas(topicNames());
  }

  /**
   * @param topics topic names
   * @return all replica in topics
   */
  List<Replica> replicas(Set<String> topics);

  /** @return all alive brokers' ids */
  default Set<Integer> brokerIds() {
    return nodes().stream().map(NodeInfo::id).collect(Collectors.toUnmodifiableSet());
  }

  /** @return all node info */
  Set<NodeInfo> nodes();

  /** @return all alive node information in the cluster */
  List<Broker> brokers();

  /** @return data folders of all broker nodes */
  default Map<Integer, Set<String>> brokerFolders() {
    return brokers().stream()
        .collect(
            Collectors.toMap(
                NodeInfo::id,
                n ->
                    n.folders().stream().map(Broker.DataFolder::path).collect(Collectors.toSet())));
  }

  /** @return a partition migrator used to move partitions to another broker or folder. */
  SyncReplicaMigrator migrator();

  /**
   * Perform preferred leader election for the specified topic/partitions. Let the first replica(the
   * preferred leader) in the partition replica list becomes the leader of its corresponding
   * topic/partition. Noted that the first replica(the preferred leader) must be in-sync state.
   * Otherwise, an exception might be raised.
   *
   * @param topicPartition to perform preferred leader election
   */
  void preferredLeaderElection(TopicPartition topicPartition);

  /** @return a snapshot object of cluster state at the moment */
  default ClusterInfo<Replica> clusterInfo() {
    return clusterInfo(topicNames());
  }

  /**
   * @param topics query only this subset of topics
   * @return a snapshot object of cluster state at the moment
   */
  default ClusterInfo<Replica> clusterInfo(Set<String> topics) {
    var nodeInfo = brokers().stream().map(n -> (NodeInfo) n).collect(Collectors.toSet());
    var replicas = Utils.packException(() -> replicas(topics));

    return new ClusterInfo<>() {
      @Override
      public Set<NodeInfo> nodes() {
        return nodeInfo;
      }

      @Override
      public Stream<Replica> replicaStream() {
        return replicas.stream();
      }
    };
  }

  /** @return all transaction ids */
  Set<String> transactionIds();

  /**
   * return transaction states associated to input ids
   *
   * @param transactionIds to query state
   * @return transaction states
   */
  List<Transaction> transactions(Set<String> transactionIds);

  List<AddingReplica> addingReplicas(Set<String> topics);

  /**
   * Delete records with offset less than specified Long
   *
   * @param recordsToDelete offset of partition
   * @return deletedRecord
   */
  Map<TopicPartition, Long> deleteRecords(Map<TopicPartition, Long> recordsToDelete);

  /** @return a utility to apply replication throttle to the cluster. */
  ReplicationThrottler replicationThrottler();

  /**
   * Clear any replication throttle related to the given topic.
   *
   * @param topic target to clear throttle.
   */
  void clearReplicationThrottle(String topic);

  /**
   * Clear any replication throttle related to the given topic/partition.
   *
   * @param topicPartition target to clear throttle.
   */
  void clearReplicationThrottle(TopicPartition topicPartition);

  /**
   * Clear any replication throttle related to the given topic/partition with specific broker id.
   *
   * @param log target to clear throttle.
   */
  void clearReplicationThrottle(TopicPartitionReplica log);

  /**
   * Clear the leader replication throttle related to the given topic/partition with specific broker
   * id.
   *
   * @param log target to clear throttle.
   */
  void clearLeaderReplicationThrottle(TopicPartitionReplica log);

  /**
   * Clear the follower replication throttle related to the given topic/partition with specific
   * broker id.
   *
   * @param log target to clear throttle.
   */
  void clearFollowerReplicationThrottle(TopicPartitionReplica log);

  /** Clear the ingress bandwidth of replication throttle for the specified brokers. */
  void clearIngressReplicationThrottle(Set<Integer> brokerIds);

  /** Clear the egress bandwidth of replication throttle for the specified brokers. */
  void clearEgressReplicationThrottle(Set<Integer> brokerIds);

  @Override
  void close();
}
