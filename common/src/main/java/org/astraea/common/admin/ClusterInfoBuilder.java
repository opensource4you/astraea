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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/** A builder with utility functions for modifying a {@link ClusterInfo}. */
public class ClusterInfoBuilder {

  private final ClusterInfo sourceCluster;
  private final List<
          BiFunction<List<Broker>, List<Replica>, Map.Entry<List<Broker>, List<Replica>>>>
      alterations;

  ClusterInfoBuilder(ClusterInfo source) {
    this.sourceCluster = source;
    this.alterations = new ArrayList<>();
  }

  /**
   * Alter the node information.
   *
   * @param alteration a function that return the new node set.
   * @return this.
   */
  public ClusterInfoBuilder applyNodes(
      BiFunction<List<Broker>, List<Replica>, List<Broker>> alteration) {
    this.alterations.add(
        (nodes, replicas) -> Map.entry(alteration.apply(nodes, replicas), replicas));
    return this;
  }

  /**
   * Alter the replica information.
   *
   * @param alteration a function that return the new replica list.
   * @return this.
   */
  public ClusterInfoBuilder applyReplicas(
      BiFunction<List<Broker>, List<Replica>, List<Replica>> alteration) {
    this.alterations.add((nodes, replicas) -> Map.entry(nodes, alteration.apply(nodes, replicas)));
    return this;
  }

  /**
   * Add fake brokers into the cluster state.
   *
   * @param brokerIds the id of fake brokers.
   * @return this.
   */
  public ClusterInfoBuilder addNode(Set<Integer> brokerIds) {
    return applyNodes(
        (nodes, replicas) -> {
          brokerIds.stream()
              .filter(newId -> nodes.stream().anyMatch(node -> newId == node.id()))
              .forEach(
                  conflict -> {
                    throw new IllegalStateException(
                        "Attempt to add broker "
                            + conflict
                            + " but another broker with this id already existed");
                  });
          return Stream.concat(nodes.stream(), brokerIds.stream().map(ClusterInfoBuilder::fakeNode))
              .toList();
        });
  }

  /**
   * Remove specific brokers from the cluster state.
   *
   * @param toRemove id to remove
   * @return this
   */
  public ClusterInfoBuilder removeNodes(Predicate<Integer> toRemove) {
    return applyNodes(
        (nodes, replicas) ->
            nodes.stream().filter(node -> toRemove.negate().test(node.id())).toList());
  }

  /**
   * Add some fake folders to a specific broker.
   *
   * @param folders the path of fake folders associated with each broker.
   * @return this.
   */
  public ClusterInfoBuilder addFolders(Map<Integer, Set<String>> folders) {
    return applyNodes(
        (nodes, replicas) -> {
          if (!folders.keySet().stream()
              .allMatch(id -> nodes.stream().anyMatch(node -> node.id() == id)))
            throw new IllegalArgumentException("Some node doesn't exists");
          return nodes.stream()
              .map(
                  node -> {
                    if (folders.containsKey(node.id()))
                      return fakeBroker(
                          node.id(),
                          node.host(),
                          node.port(),
                          Stream.concat(
                                  node.dataFolders().stream(),
                                  folders.get(node.id()).stream()
                                      .map(ClusterInfoBuilder::fakeDataFolder))
                              .toList());
                    else return node;
                  })
              .toList();
        });
  }

  /**
   * Add a fake topic to the cluster.
   *
   * @param topicName the name of the topic.
   * @param partitionSize the size of the partition.
   * @param replicaFactor the number of replica for each partition.
   * @return this.
   */
  public ClusterInfoBuilder addTopic(String topicName, int partitionSize, short replicaFactor) {
    return addTopic(topicName, partitionSize, replicaFactor, x -> x);
  }

  /**
   * Add a fake topic to the cluster.
   *
   * @param topicName the name of the topic.
   * @param partitionSize the size of the partition.
   * @param replicaFactor the number of replica for each partition.
   * @param mapper modification applied to the newly created replicas.
   * @return this.
   */
  public ClusterInfoBuilder addTopic(
      String topicName, int partitionSize, short replicaFactor, Function<Replica, Replica> mapper) {
    return applyReplicas(
        (nodes, replicas) -> {
          if (nodes.size() < replicaFactor)
            throw new IllegalArgumentException(
                "Insufficient node for this replica factor: "
                    + nodes.size()
                    + " < "
                    + replicaFactor);
          var nodeSelector = Stream.generate(nodes::stream).flatMap(x -> x).iterator();

          // simulate the actual Kafka logic of log placement
          var folderLogCounter =
              nodes.stream()
                  .collect(
                      Collectors.toUnmodifiableMap(
                          Broker::id,
                          node ->
                              node.dataFolders().stream()
                                  .collect(
                                      Collectors.toMap(
                                          Broker.DataFolder::path, x -> new AtomicInteger()))));
          replicas.forEach(
              replica ->
                  folderLogCounter.get(replica.brokerId()).get(replica.path()).incrementAndGet());

          folderLogCounter.forEach(
              (node, folders) -> {
                if (folders.size() == 0)
                  throw new IllegalArgumentException("This broker has no folder: " + node);
              });

          Stream<Replica> newTopic =
              IntStream.range(0, partitionSize)
                  .mapToObj(partition -> TopicPartition.of(topicName, partition))
                  .flatMap(
                      tp ->
                          IntStream.range(0, replicaFactor)
                              .mapToObj(
                                  index -> {
                                    final Broker broker = nodeSelector.next();
                                    final String path =
                                        folderLogCounter.get(broker.id()).entrySet().stream()
                                            .min(Comparator.comparing(x -> x.getValue().get()))
                                            .map(
                                                entry -> {
                                                  entry.getValue().incrementAndGet();
                                                  return entry.getKey();
                                                })
                                            .orElseThrow();

                                    return Replica.builder()
                                        .topic(tp.topic())
                                        .partition(tp.partition())
                                        .brokerId(broker.id())
                                        .isAdding(false)
                                        .isRemoving(false)
                                        .lag(0)
                                        .isInternal(false)
                                        .isLeader(index == 0)
                                        .isSync(true)
                                        .isFuture(false)
                                        .isOffline(false)
                                        .isPreferredLeader(index == 0)
                                        .path(path)
                                        .build();
                                  }))
                  .map(mapper);

          return Stream.concat(replicas.stream(), newTopic).toList();
        });
  }

  /**
   * Apply alteration to specific replicas.
   *
   * @param mapper modification applied to the matched replica.
   * @return this.
   */
  public ClusterInfoBuilder mapLog(Function<Replica, Replica> mapper) {
    return applyReplicas((nodes, replicas) -> replicas.stream().map(mapper).toList());
  }

  /**
   * Assign the specific replica to another location in the cluster.
   *
   * @param replica the replica to reassign.
   * @param toBroker the new broker for the replica being reassigned.
   * @param toDir the new data folder of that broker for the replica being reassigned.
   * @return this.
   */
  public ClusterInfoBuilder reassignReplica(
      TopicPartitionReplica replica, int toBroker, String toDir) {
    Objects.requireNonNull(toDir);
    return applyReplicas(
        (nodes, replicas) -> {
          var newNode =
              nodes.stream()
                  .filter(n -> n.id() == toBroker)
                  .findFirst()
                  .orElseThrow(() -> new IllegalArgumentException("No such replica: " + toBroker));
          var matched = new AtomicBoolean(false);
          var collect =
              replicas.stream()
                  .map(
                      r -> {
                        if (r.topicPartitionReplica().equals(replica)) {
                          matched.set(true);
                          return Replica.builder(r).brokerId(newNode.id()).path(toDir).build();
                        } else {
                          return r;
                        }
                      })
                  .toList();
          if (!matched.get()) throw new IllegalArgumentException("No such replica: " + replica);
          return collect;
        });
  }

  /**
   * Let the specific replica become the preferred leader of its partition.
   *
   * @param replica the replica to become preferred leader.
   * @return this.
   */
  public ClusterInfoBuilder setPreferredLeader(TopicPartitionReplica replica) {
    return applyReplicas(
        (nodes, replicas) -> {
          var matched = new AtomicBoolean(false);
          var collect =
              replicas.stream()
                  .map(
                      r -> {
                        if (r.topicPartitionReplica().equals(replica)) {
                          matched.set(true);
                          return Replica.builder(r).isLeader(true).isPreferredLeader(true).build();
                        } else if (r.topicPartition().equals(replica.topicPartition())
                            && (r.isPreferredLeader() || r.isLeader())) {
                          return Replica.builder(r)
                              .isLeader(false)
                              .isPreferredLeader(false)
                              .build();
                        } else {
                          return r;
                        }
                      })
                  .toList();
          if (!matched.get()) throw new IllegalArgumentException("No such replica: " + replica);

          return collect;
        });
  }

  /**
   * Apply the pending alteration to the source cluster, and return the transformed {@link
   * ClusterInfo}.
   */
  public ClusterInfo build() {
    var nodes = sourceCluster.brokers();
    var replicas = sourceCluster.replicas();
    for (var alteration : alterations) {
      var e = alteration.apply(nodes, replicas);
      nodes = e.getKey();
      replicas = e.getValue();
    }
    // TODO: support adding custom topic config to ClusterInfoBuilder
    return ClusterInfo.of(sourceCluster.clusterId(), nodes, sourceCluster.topics(), replicas);
  }

  private static Broker fakeNode(int brokerId) {
    var host = "fake-node-" + brokerId;
    var port = new Random(brokerId).nextInt(65535) + 1;
    var folders = List.<Broker.DataFolder>of();

    return fakeBroker(brokerId, host, port, folders);
  }

  static Broker fakeBroker(int Id, String host, int port, List<Broker.DataFolder> dataFolders) {
    return new Broker(Id, host, port, false, Config.EMPTY, dataFolders, Set.of(), Set.of());
  }

  private static Broker.DataFolder fakeDataFolder(String path) {
    return new Broker.DataFolder(path, Map.of(), Map.of());
  }
}
