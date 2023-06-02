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
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.astraea.common.Lazy;

/** It optimizes all queries by pre-allocated Map collection. */
class OptimizedClusterInfo implements ClusterInfo {
  private final String clusterId;
  private final List<Broker> brokers;
  private final List<Replica> all;

  private final Lazy<Map<String, Topic>> topics;

  private final Lazy<Map<BrokerTopic, List<Replica>>> byBrokerTopic;

  private final Lazy<Map<BrokerTopic, List<Replica>>> byBrokerTopicForLeader;
  private final Lazy<Map<Integer, List<Replica>>> byBroker;
  private final Lazy<Map<String, List<Replica>>> byTopic;

  private final Lazy<Map<String, List<Replica>>> byTopicForLeader;
  private final Lazy<Map<TopicPartition, List<Replica>>> byPartition;
  private final Lazy<Map<TopicPartitionReplica, List<Replica>>> byReplica;

  OptimizedClusterInfo(
      String clusterId, List<Broker> brokers, Map<String, Topic> topics, List<Replica> replicas) {
    this.clusterId = clusterId;
    this.brokers = brokers;
    this.all = replicas;
    this.topics =
        Lazy.of(
            () ->
                replicas.stream()
                    .map(Replica::topic)
                    .distinct()
                    .map(
                        topic ->
                            new Topic(
                                topic,
                                Optional.ofNullable(topics.get(topic))
                                    .map(Topic::config)
                                    .orElse(Config.EMPTY),
                                Optional.ofNullable(topics.get(topic))
                                    .map(Topic::internal)
                                    .orElse(false),
                                OptimizedClusterInfo.this.replicas(topic).stream()
                                    .map(r -> r.topicPartition().partition())
                                    .collect(Collectors.toUnmodifiableSet())))
                    .collect(Collectors.toUnmodifiableMap(Topic::name, t -> t)));
    this.byBrokerTopic =
        Lazy.of(
            () ->
                all.stream()
                    .collect(
                        Collectors.groupingBy(
                            r -> BrokerTopic.of(r.brokerId(), r.topic()),
                            Collectors.toUnmodifiableList())));
    this.byBrokerTopicForLeader =
        Lazy.of(
            () ->
                all.stream()
                    .filter(Replica::isOnline)
                    .filter(Replica::isLeader)
                    .collect(
                        Collectors.groupingBy(
                            r -> BrokerTopic.of(r.brokerId(), r.topic()),
                            Collectors.toUnmodifiableList())));

    this.byBroker =
        Lazy.of(
            () ->
                all.stream()
                    .collect(
                        Collectors.groupingBy(r -> r.brokerId(), Collectors.toUnmodifiableList())));

    this.byTopic =
        Lazy.of(
            () ->
                all.stream()
                    .collect(
                        Collectors.groupingBy(Replica::topic, Collectors.toUnmodifiableList())));

    this.byTopicForLeader =
        Lazy.of(
            () ->
                all.stream()
                    .filter(Replica::isOnline)
                    .filter(Replica::isLeader)
                    .collect(
                        Collectors.groupingBy(Replica::topic, Collectors.toUnmodifiableList())));

    this.byPartition =
        Lazy.of(
            () ->
                all.stream()
                    .collect(
                        Collectors.groupingBy(
                            Replica::topicPartition, Collectors.toUnmodifiableList())));

    this.byReplica =
        Lazy.of(
            () ->
                all.stream()
                    .collect(
                        Collectors.groupingBy(
                            Replica::topicPartitionReplica, Collectors.toUnmodifiableList())));
  }

  @Override
  public Stream<Replica> replicaStream(String topic) {
    return byTopic.get().getOrDefault(topic, List.of()).stream();
  }

  @Override
  public Stream<Replica> replicaStream(TopicPartition partition) {
    return byPartition.get().getOrDefault(partition, List.of()).stream();
  }

  @Override
  public Stream<Replica> replicaStream(TopicPartitionReplica replica) {
    return byReplica.get().getOrDefault(replica, List.of()).stream();
  }

  @Override
  public Stream<Replica> replicaStream(int broker) {
    return byBroker.get().getOrDefault(broker, List.of()).stream();
  }

  @Override
  public Stream<Replica> replicaStream(BrokerTopic brokerTopic) {
    return byBrokerTopic.get().getOrDefault(brokerTopic, List.of()).stream();
  }

  @Override
  public Set<TopicPartition> topicPartitions() {
    return byPartition.get().keySet();
  }

  @Override
  public Set<TopicPartitionReplica> topicPartitionReplicas() {
    return byReplica.get().keySet();
  }

  @Override
  public String clusterId() {
    return clusterId;
  }

  @Override
  public Set<String> topicNames() {
    return byTopic.get().keySet();
  }

  @Override
  public List<Broker> brokers() {
    return brokers;
  }

  @Override
  public Stream<Replica> replicaStream() {
    return all.stream();
  }

  @Override
  public Map<String, Topic> topics() {
    return topics.get();
  }

  @Override
  public List<Replica> replicas() {
    return all;
  }

  @Override
  public List<Replica> replicas(String topic) {
    return byTopic.get().getOrDefault(topic, List.of());
  }

  @Override
  public List<Replica> replicas(TopicPartition topicPartition) {
    return byPartition.get().getOrDefault(topicPartition, List.of());
  }

  @Override
  public List<Replica> replicas(TopicPartitionReplica replica) {
    return byReplica.get().getOrDefault(replica, List.of());
  }

  @Override
  public List<Replica> replicaLeaders(String topic) {
    return byTopicForLeader.get().getOrDefault(topic, List.of());
  }

  @Override
  public List<Replica> replicaLeaders(BrokerTopic brokerTopic) {
    return byBrokerTopicForLeader.get().getOrDefault(brokerTopic, List.of());
  }
}
