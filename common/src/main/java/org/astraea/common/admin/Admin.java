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
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.CompletionStage;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.astraea.common.DataRate;
import org.astraea.common.FutureUtils;
import org.astraea.common.Utils;
import org.astraea.common.consumer.Consumer;
import org.astraea.common.consumer.IteratorLimit;
import org.astraea.common.consumer.Record;
import org.astraea.common.consumer.SeekStrategy;

public interface Admin extends AutoCloseable {

  static Admin of(String bootstrap) {
    return of(Map.of(AdminConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrap));
  }

  static Admin of(Map<String, String> configs) {
    return new AdminImpl(configs);
  }

  // ---------------------------------[internal]---------------------------------//
  String clientId();

  /**
   * @return the number of pending requests.
   */
  int runningRequests();

  // ---------------------------------[readonly]---------------------------------//

  /**
   * @param listInternal should list internal topics or not
   * @return names of topics
   */
  CompletionStage<Set<String>> topicNames(boolean listInternal);

  /**
   * @return names of internal topics
   */
  CompletionStage<Set<String>> internalTopicNames();

  /**
   * Find out the topic names matched to input checkers.
   *
   * @param checkers used to predicate topic
   * @return topic names accepted by all given checkers
   */
  default CompletionStage<Set<String>> topicNames(List<TopicChecker> checkers) {
    if (checkers.isEmpty()) return topicNames(false);
    return topicNames(false)
        .thenCompose(
            topicNames ->
                FutureUtils.sequence(
                        checkers.stream()
                            .map(checker -> checker.test(this, topicNames).toCompletableFuture())
                            .collect(Collectors.toUnmodifiableList()))
                    .thenApply(
                        all ->
                            all.stream()
                                .flatMap(Collection::stream)
                                // return topics accepted by all checkers
                                .filter(t -> all.stream().allMatch(ts -> ts.contains(t)))
                                .collect(Collectors.toUnmodifiableSet())));
  }

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

  CompletionStage<Map<TopicPartition, Long>> earliestOffsets(Set<TopicPartition> topicPartitions);

  CompletionStage<Map<TopicPartition, Long>> latestOffsets(Set<TopicPartition> topicPartitions);

  /**
   * find the timestamp of the latest record for given partitions
   *
   * @param topicPartitions to search timestamp of the latest record
   * @param timeout to wait the latest record
   * @return partition and timestamp of the latest record
   */
  default CompletionStage<Map<TopicPartition, Long>> timestampOfLatestRecords(
      Set<TopicPartition> topicPartitions, Duration timeout) {
    return latestRecords(topicPartitions, 1, timeout)
        .thenApply(
            records ->
                records.entrySet().stream()
                    .collect(
                        Collectors.toMap(
                            Map.Entry::getKey,
                            e ->
                                e.getValue().stream()
                                    .mapToLong(Record::timestamp)
                                    .max()
                                    // the value CANNOT be empty
                                    .getAsLong())));
  }

  default CompletionStage<Map<TopicPartition, List<Record<byte[], byte[]>>>> latestRecords(
      Set<TopicPartition> topicPartitions, int records, Duration timeout) {
    return bootstrapServers()
        .thenApply(
            bootstrap ->
                StreamSupport.stream(
                        Spliterators.spliteratorUnknownSize(
                            Consumer.forPartitions(topicPartitions)
                                .bootstrapServers(bootstrap)
                                .seek(SeekStrategy.DISTANCE_FROM_LATEST, records)
                                .iterator(
                                    List.of(
                                        IteratorLimit.count(topicPartitions.size() * records),
                                        IteratorLimit.elapsed(timeout))),
                            Spliterator.ORDERED),
                        false)
                    .collect(
                        Collectors.groupingBy(r -> TopicPartition.of(r.topic(), r.partition()))));
  }

  /**
   * find the max timestamp of existent records for given partitions
   *
   * @param topicPartitions to search max timestamp
   * @return partition and max timestamp
   */
  CompletionStage<Map<TopicPartition, Long>> maxTimestamps(Set<TopicPartition> topicPartitions);

  CompletionStage<List<Partition>> partitions(Set<String> topics);

  /**
   * @return online broker information
   */
  CompletionStage<List<Broker>> brokers();

  default CompletionStage<String> bootstrapServers() {
    return brokers()
        .thenApply(
            bs -> bs.stream().map(b -> b.host() + ":" + b.port()).collect(Collectors.joining(",")));
  }

  default CompletionStage<Map<Integer, Set<String>>> brokerFolders() {
    return brokers()
        .thenApply(
            brokers -> brokers.stream().collect(Collectors.toMap(Broker::id, Broker::dataFolders)));
  }

  CompletionStage<Set<String>> consumerGroupIds();

  CompletionStage<List<ConsumerGroup>> consumerGroups(Set<String> consumerGroupIds);

  CompletionStage<List<ProducerState>> producerStates(Set<TopicPartition> partitions);

  CompletionStage<Set<String>> transactionIds();

  CompletionStage<List<Transaction>> transactions(Set<String> transactionIds);

  CompletionStage<ClusterInfo> clusterInfo(Set<String> topics);

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

  /**
   * @return a topic creator to set all topic configs and then run the procedure.
   */
  TopicCreator creator();

  CompletionStage<Void> moveToBrokers(Map<TopicPartition, List<Integer>> assignments);

  CompletionStage<Void> moveToFolders(Map<TopicPartitionReplica, String> assignments);

  /**
   * Declare the preferred data folder for the designated partition at a specific broker.
   *
   * <p>Preferred data folder is the data folder a partition will use when it is being placed on the
   * specific broker({@link Admin#moveToBrokers(Map)}). This API won't trigger a folder-to-folder
   * replica movement. To Perform folder-to-folder movement, consider use {@link
   * Admin#moveToFolders(Map)}.
   *
   * <p>This API is not transactional. It may alter folders if the target broker has out-of-date
   * metadata or running reassignments
   */
  CompletionStage<Void> declarePreferredDataFolders(Map<TopicPartitionReplica, String> assignments);

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

  /**
   * @param override defines the key and new value. The other undefined keys won't get changed.
   */
  CompletionStage<Void> setTopicConfigs(Map<String, Map<String, String>> override);

  /**
   * append the value to config. Noted that it appends nothing if the existent value is "*".
   *
   * @param appended values
   */
  CompletionStage<Void> appendTopicConfigs(Map<String, Map<String, String>> appended);

  /**
   * subtract the value to config. Noted that it throws exception if the existent value is "*".
   *
   * @param subtract values
   */
  CompletionStage<Void> subtractTopicConfigs(Map<String, Map<String, String>> subtract);

  /**
   * unset the value associated to given keys. The unset config will become either null of default
   * value. Normally, the default value is defined by server.properties or hardcode in source code.
   */
  CompletionStage<Void> unsetTopicConfigs(Map<String, Set<String>> unset);

  /**
   * @param override defines the key and new value. The other undefined keys won't get changed.
   */
  CompletionStage<Void> setBrokerConfigs(Map<Integer, Map<String, String>> override);

  /**
   * unset the value associated to given keys. The unset config will become either null of default
   * value. Normally, the default value is defined by server.properties or hardcode in source code.
   */
  CompletionStage<Void> unsetBrokerConfigs(Map<Integer, Set<String>> unset);

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
                  .filter(Replica::isLeader)
                  .collect(Collectors.groupingBy(Replica::topic));
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
                .allMatch(Replica::isLeader),
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
                .allMatch(r -> r.isSync() && !r.isFuture()),
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
      Set<String> topics, Predicate<ClusterInfo> predicate, Duration timeout, int debounce) {
    return Utils.loop(
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
        debounce);
  }

  @Override
  void close();
}
