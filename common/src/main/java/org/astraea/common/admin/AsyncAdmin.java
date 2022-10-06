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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import org.apache.kafka.clients.CommonClientConfigs;

public interface AsyncAdmin extends AutoCloseable {

  static AsyncAdmin of(String bootstrap) {
    return of(Map.of(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrap));
  }

  static AsyncAdmin of(Map<String, Object> configs) {
    return new AsyncAdminImpl(configs);
  }

  // ---------------------------------[internal]---------------------------------//
  String clientId();

  /** @return the number of pending requests. */
  int pendingRequests();

  // ---------------------------------[readonly]---------------------------------//

  /**
   * @param listInternal should list internal topics or not
   * @return names of topics
   */
  CompletionStage<Set<String>> topicNames(boolean listInternal);

  CompletionStage<List<Topic>> topics(Set<String> topics);

  /** delete topics by topic names */
  CompletionStage<Void> deleteTopics(Set<String> topics);

  /**
   * @param topics target
   * @return the partitions belong to input topics
   */
  CompletionStage<Set<TopicPartition>> topicPartitions(Set<String> topics);

  /**
   * list all partitions belongs to input brokers
   *
   * @param brokerId to search
   * @return all partition belongs to brokers
   */
  CompletionStage<Set<TopicPartition>> topicPartitions(int brokerId);

  CompletionStage<List<Partition>> partitions(Set<String> topics);

  CompletionStage<Set<NodeInfo>> nodeInfos();

  CompletionStage<List<Broker>> brokers();

  default CompletionStage<Map<Integer, Set<String>>> brokerFolders() {
    return brokers()
        .thenApply(
            brokers ->
                brokers.stream()
                    .collect(
                        Collectors.toMap(
                            NodeInfo::id,
                            n ->
                                n.folders().stream()
                                    .map(Broker.DataFolder::path)
                                    .collect(Collectors.toSet()))));
  }

  CompletionStage<Set<String>> consumerGroupIds();

  CompletionStage<List<ConsumerGroup>> consumerGroups(Set<String> consumerGroupIds);

  CompletionStage<List<ProducerState>> producerStates(Set<TopicPartition> partitions);

  CompletionStage<List<AddingReplica>> addingReplicas(Set<String> topics);

  CompletionStage<Set<String>> transactionIds();

  CompletionStage<List<Transaction>> transactions(Set<String> transactionIds);

  CompletionStage<List<Replica>> replicas(Set<String> topics);

  default CompletionStage<ClusterInfo<Replica>> clusterInfo(Set<String> topics) {
    return nodeInfos().thenCombine(replicas(topics), ClusterInfo::of);
  }

  // ---------------------------------[write]---------------------------------//

  /** @return a topic creator to set all topic configs and then run the procedure. */
  TopicCreator creator();

  /** @return a partition migrator used to move partitions to another broker or folder. */
  ReplicaMigrator migrator();

  /**
   * Perform preferred leader election for the specified topic/partitions. Let the first replica(the
   * preferred leader) in the partition replica list becomes the leader of its corresponding
   * topic/partition. Noted that the first replica(the preferred leader) must be in-sync state.
   * Otherwise, an exception might be raised.
   *
   * @param topicPartition to perform preferred leader election
   */
  CompletionStage<Void> preferredLeaderElection(TopicPartition topicPartition);

  @Override
  void close();
}
