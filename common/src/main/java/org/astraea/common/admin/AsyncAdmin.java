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

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.kafka.clients.CommonClientConfigs;
import org.astraea.common.DataRate;
import org.astraea.common.Utils;

public interface AsyncAdmin extends AutoCloseable {

  static AsyncAdmin of(String bootstrap) {
    return of(Map.of(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrap));
  }

  static AsyncAdmin of(Map<String, String> configs) {
    return new AsyncAdminImpl(configs);
  }

  // ---------------------------------[internal]---------------------------------//
  String clientId();

  /** @return the number of pending requests. */
  int runningRequests();

  // ---------------------------------[readonly]---------------------------------//

  /**
   * @param listInternal should list internal topics or not
   * @return names of topics
   */
  CompletionStage<Set<String>> topicNames(boolean listInternal);

  CompletionStage<List<Topic>> topics(Set<String> topics);

  /**
   * @param topics target
   * @return the partitions belong to input topics
   */
  CompletionStage<Set<TopicPartition>> topicPartitions(Set<String> topics);

  /**
   * list all partition replicas belongs to input brokers
   *
   * @param brokers to search
   * @return all partition belongs to brokers
   */
  CompletionStage<Set<TopicPartitionReplica>> topicPartitionReplicas(Set<Integer> brokers);

  CompletionStage<List<Partition>> partitions(Set<String> topics);

  /** @return online node information */
  CompletionStage<Set<NodeInfo>> nodeInfos();

  /** @return online broker information */
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

  default CompletionStage<Set<String>> idleTopic(List<TopicChecker> checkers) {
    if (checkers.isEmpty()) {
      throw new RuntimeException("Can not check for idle topics because of no checkers!");
    }

    return topicNames(false)
        .thenCompose(
            topicNames ->
                Utils.sequence(
                        checkers.stream()
                            .map(
                                checker ->
                                    checker.usedTopics(this, topicNames).toCompletableFuture())
                            .collect(Collectors.toUnmodifiableList()))
                    .thenApply(
                        s ->
                            s.stream()
                                .flatMap(Collection::stream)
                                .collect(Collectors.toUnmodifiableSet()))
                    .thenApply(
                        usedTopics ->
                            topicNames.stream()
                                .filter(name -> !usedTopics.contains(name))
                                .collect(Collectors.toUnmodifiableSet())));
  }

  /**
   * get the quotas associated to given target keys and target values. The available target types
   * include {@link QuotaConfigs#IP}, {@link QuotaConfigs#CLIENT_ID}, and {@link QuotaConfigs#USER}
   *
   * @param targets target type and associated value. For example: Map.of({@link QuotaConfigs#IP},
   *     Set.of(10.1.1.2, 10..2.2.2))
   * @return quotas matched to given target
   */
  CompletionStage<List<Quota>> quotas(Map<String, Set<String>> targets);

  /**
   * get the quotas associated to given target keys. The available target types include {@link
   * QuotaConfigs#IP}, {@link QuotaConfigs#CLIENT_ID}, and {@link QuotaConfigs#USER}
   *
   * @param targetKeys target keys
   * @return quotas matched to given target
   */
  CompletionStage<List<Quota>> quotas(Set<String> targetKeys);

  CompletionStage<List<Quota>> quotas();

  // ---------------------------------[write]---------------------------------//

  /**
   * set the connection rate for given ip address.
   *
   * @param ipAndRate ip address and its connection rate
   */
  CompletionStage<Void> setConnectionQuotas(Map<String, Integer> ipAndRate);

  /**
   * remove the connection quotas for given ip addresses
   *
   * @param ips to delete connection quotas
   */
  CompletionStage<Void> unsetConnectionQuotas(Set<String> ips);

  /**
   * set the producer rate for given client id
   *
   * @param clientAndRate client id and its producer rate
   */
  CompletionStage<Void> setProducerQuotas(Map<String, DataRate> clientAndRate);

  /**
   * remove the producer rate quotas for given client ids
   *
   * @param clientIds to delete producer rate quotas
   */
  CompletionStage<Void> unsetProducerQuotas(Set<String> clientIds);

  /**
   * set the consumer rate for given client id
   *
   * @param clientAndRate client id and its consumer rate
   */
  CompletionStage<Void> setConsumerQuotas(Map<String, DataRate> clientAndRate);

  /**
   * remove the consumer rate quotas for given client ids
   *
   * @param clientIds to delete consumer rate quotas
   */
  CompletionStage<Void> unsetConsumerQuotas(Set<String> clientIds);

  /** @return a topic creator to set all topic configs and then run the procedure. */
  TopicCreator creator();

  CompletionStage<Void> moveToBrokers(Map<TopicPartition, List<Integer>> assignments);

  CompletionStage<Void> moveToFolders(Map<TopicPartitionReplica, String> assignments);

  /**
   * Perform preferred leader election for the specified topic/partitions. Let the first replica(the
   * preferred leader) in the partition replica list becomes the leader of its corresponding
   * topic/partition. Noted that the first replica(the preferred leader) must be in-sync state.
   * Otherwise, an exception might be raised.
   *
   * @param topicPartitions to perform preferred leader election
   */
  CompletionStage<Void> preferredLeaderElection(Set<TopicPartition> topicPartitions);

  /**
   * @param total the final number of partitions. Noted that reducing number of partitions is
   *     illegal
   */
  CompletionStage<Void> addPartitions(String topic, int total);

  /** @param override defines the key and new value. The other undefined keys won't get changed. */
  CompletionStage<Void> setConfigs(String topic, Map<String, String> override);

  /**
   * unset the value associated to given keys. The unset config will become either null of default
   * value. Normally, the default value is defined by server.properties or hardcode in source code.
   */
  CompletionStage<Void> unsetConfigs(String topic, Set<String> keys);

  /** @param override defines the key and new value. The other undefined keys won't get changed. */
  CompletionStage<Void> setConfigs(int brokerId, Map<String, String> override);

  /**
   * unset the value associated to given keys. The unset config will become either null of default
   * value. Normally, the default value is defined by server.properties or hardcode in source code.
   */
  CompletionStage<Void> unsetConfigs(int brokerId, Set<String> keys);

  /** delete topics by topic names */
  CompletionStage<Void> deleteTopics(Set<String> topics);

  /**
   * Remove the records when their offsets are smaller than given offsets.
   *
   * @param offsets to truncate topic partition
   * @return topic partition and low watermark (it means the minimum logStartOffset of all alive
   *     replicas)
   */
  CompletionStage<Map<TopicPartition, Long>> deleteRecords(Map<TopicPartition, Long> offsets);

  /**
   * delete all members from given groups.
   *
   * @param groupAndInstanceIds to delete instance id from group (key)
   */
  CompletionStage<Void> deleteInstanceMembers(Map<String, Set<String>> groupAndInstanceIds);

  /**
   * delete all members from given groups.
   *
   * @param consumerGroups to delete members
   */
  CompletionStage<Void> deleteMembers(Set<String> consumerGroups);

  /**
   * delete given groups. all members in those groups get deleted also.
   *
   * @param consumerGroups to be deleted
   */
  CompletionStage<Void> deleteGroups(Set<String> consumerGroups);

  // ---------------------------------[wait]---------------------------------//

  /**
   * wait the leader of partition get allocated. The topic creation needs time to be synced by all
   * brokers. This method is used to check whether "most" of brokers get the topic creation.
   *
   * @param topicAndNumberOfPartitions to check leader allocation
   * @param timeout to wait
   * @return a background thread used to check leader election.
   */
  default CompletionStage<Boolean> waitPartitionLeaderSynced(
      Map<String, Integer> topicAndNumberOfPartitions, Duration timeout) {
    return waitCluster(
        topicAndNumberOfPartitions.keySet(),
        clusterInfo -> {
          var current =
              clusterInfo
                  .replicaStream()
                  .filter(ReplicaInfo::isLeader)
                  .collect(Collectors.groupingBy(ReplicaInfo::topic));
          return topicAndNumberOfPartitions.entrySet().stream()
              .allMatch(
                  entry ->
                      current.getOrDefault(entry.getKey(), List.of()).size() == entry.getValue());
        },
        timeout,
        2);
  }

  /**
   * wait the preferred leader of partition get elected.
   *
   * @param topicPartitions to check leader election
   * @param timeout to wait
   * @return a background thread used to check leader election.
   */
  default CompletionStage<Boolean> waitPreferredLeaderSynced(
      Set<TopicPartition> topicPartitions, Duration timeout) {
    return waitCluster(
        topicPartitions.stream().map(TopicPartition::topic).collect(Collectors.toSet()),
        clusterInfo ->
            clusterInfo
                .replicaStream()
                .filter(r -> topicPartitions.contains(r.topicPartition()))
                .filter(Replica::isPreferredLeader)
                .allMatch(ReplicaInfo::isLeader),
        timeout,
        2);
  }

  /**
   * wait the given replicas to be allocated correctly
   *
   * @param replicas the expected replica allocations
   * @param timeout to wait
   * @return a background thread used to check replica allocations
   */
  default CompletionStage<Boolean> waitReplicasSynced(
      Set<TopicPartitionReplica> replicas, Duration timeout) {
    return waitCluster(
        replicas.stream().map(TopicPartitionReplica::topic).collect(Collectors.toSet()),
        clusterInfo ->
            clusterInfo
                .replicaStream()
                .filter(r -> replicas.contains(r.topicPartitionReplica()))
                .allMatch(r -> r.inSync() && !r.isFuture()),
        timeout,
        2);
  }

  /**
   * wait the async operations to be done on server-side. You have to define the predicate to
   * terminate loop. Or the loop get breaks when timeout is reached.
   *
   * @param topics to trace
   * @param predicate to break loop
   * @param timeout to break loop
   * @param debounce to double-check the status. Some brokers may return out-of-date cluster state,
   *     so you can set a positive value to keep the loop until to debounce is completed
   * @return a background running loop
   */
  default CompletionStage<Boolean> waitCluster(
      Set<String> topics,
      Predicate<ClusterInfo<Replica>> predicate,
      Duration timeout,
      int debounce) {
    return loop(
        () ->
            clusterInfo(topics)
                .thenApply(predicate::test)
                .exceptionally(
                    e -> {
                      if (e
                              instanceof
                              org.apache.kafka.common.errors.UnknownTopicOrPartitionException
                          || e.getCause()
                              instanceof
                              org.apache.kafka.common.errors.UnknownTopicOrPartitionException)
                        return false;
                      if (e instanceof RuntimeException) throw (RuntimeException) e;
                      throw new RuntimeException(e);
                    }),
        timeout.toMillis(),
        debounce,
        debounce);
  }

  static CompletionStage<Boolean> loop(
      Supplier<CompletionStage<Boolean>> supplier,
      long remainingMs,
      final int debounce,
      int remainingDebounce) {
    if (remainingMs <= 0) return CompletableFuture.completedFuture(false);
    var start = System.currentTimeMillis();
    return supplier
        .get()
        .thenCompose(
            match -> {
              // everything is good!!!
              if (match && remainingDebounce <= 0) return CompletableFuture.completedFuture(true);

              // take a break before retry/debounce
              Utils.sleep(Duration.ofMillis(300));

              var remaining = remainingMs - (System.currentTimeMillis() - start);

              // keep debounce
              if (match) return loop(supplier, remaining, debounce, remainingDebounce - 1);

              // reset debounce for retry
              return loop(supplier, remaining, debounce, debounce);
            });
  }

  @Override
  void close();
}
