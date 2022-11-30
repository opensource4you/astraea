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
import java.util.Collections;
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
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/** A builder with utility functions for modifying a {@link ClusterInfo}. */
public class ClusterInfoBuilder {

  private final ClusterInfo<Replica> sourceCluster;
  private final List<
          BiFunction<Set<NodeInfo>, List<Replica>, Map.Entry<Set<NodeInfo>, List<Replica>>>>
      alterations;

  private ClusterInfoBuilder(ClusterInfo<Replica> source) {
    this.sourceCluster = source;
    this.alterations = new ArrayList<>();
  }

  public static ClusterInfoBuilder builder() {
    return builder(ClusterInfo.empty());
  }

  public static ClusterInfoBuilder builder(ClusterInfo<Replica> source) {
    return new ClusterInfoBuilder(source);
  }

  /**
   * Alter the node information.
   *
   * @param alteration a function that return the new node set.
   * @return this.
   */
  public ClusterInfoBuilder applyNodes(
      BiFunction<Set<NodeInfo>, List<Replica>, Set<NodeInfo>> alteration) {
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
      BiFunction<Set<NodeInfo>, List<Replica>, List<Replica>> alteration) {
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
        (nodes, replicas) ->
            Stream.concat(nodes.stream(), brokerIds.stream().map(ClusterInfoBuilder::fakeNode))
                .collect(Collectors.toUnmodifiableSet()));
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
                      return FakeBroker.of(
                          node.id(),
                          node.host(),
                          node.port(),
                          Stream.concat(
                                  ((Broker) node).dataFolders().stream(),
                                  folders.get(node.id()).stream().map(FakeDataFolder::of))
                              .collect(Collectors.toUnmodifiableList()));
                    else return node;
                  })
              .collect(Collectors.toUnmodifiableSet());
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
          if (nodes.stream().anyMatch(node -> !(node instanceof Broker)))
            throw new IllegalStateException("All the nodes must include the folder info");
          if (nodes.size() < replicaFactor)
            throw new IllegalArgumentException(
                "Insufficient node for this replica factor: "
                    + nodes.size()
                    + " < "
                    + replicaFactor);
          var nodeSelector =
              Stream.generate(nodes::stream).flatMap(x -> x).map(x -> (Broker) x).iterator();

          // simulate the actual Kafka logic of log placement
          var folderLogCounter =
              nodes.stream()
                  .collect(
                      Collectors.toUnmodifiableMap(
                          node -> node,
                          node ->
                              ((Broker) node)
                                  .dataFolders().stream()
                                      .collect(
                                          Collectors.toMap(
                                              Broker.DataFolder::path, x -> new AtomicInteger()))));
          replicas.forEach(
              replica ->
                  folderLogCounter.get(replica.nodeInfo()).get(replica.path()).incrementAndGet());

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
                                        folderLogCounter.get(broker).entrySet().stream()
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
                                        .nodeInfo(broker)
                                        .isAdding(false)
                                        .isRemoving(false)
                                        .lag(0)
                                        .internal(false)
                                        .isLeader(index == 0)
                                        .inSync(true)
                                        .isFuture(false)
                                        .isOffline(false)
                                        .isPreferredLeader(index == 0)
                                        .path(path)
                                        .build();
                                  }))
                  .map(mapper);

          return Stream.concat(replicas.stream(), newTopic)
              .collect(Collectors.toUnmodifiableList());
        });
  }

  /**
   * Apply alteration to specific replicas.
   *
   * @param mapper modification applied to the matched replica.
   * @return this.
   */
  public ClusterInfoBuilder mapLog(Function<Replica, Replica> mapper) {
    return applyReplicas(
        (nodes, replicas) ->
            replicas.stream().map(mapper).collect(Collectors.toUnmodifiableList()));
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
                          return Replica.builder(r).nodeInfo(newNode).path(toDir).build();
                        } else {
                          return r;
                        }
                      })
                  .collect(Collectors.toUnmodifiableList());
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
                  .collect(Collectors.toUnmodifiableList());
          if (!matched.get()) throw new IllegalArgumentException("No such replica: " + replica);

          return collect;
        });
  }

  /**
   * Apply the pending alteration to the source cluster, and return the transformed {@link
   * ClusterInfo}.
   */
  public ClusterInfo<Replica> build() {
    var nodes = sourceCluster.nodes();
    var replicas = sourceCluster.replicas();
    for (var alteration : alterations) {
      var e = alteration.apply(nodes, replicas);
      nodes = e.getKey();
      replicas = e.getValue();
    }
    return ClusterInfo.of(nodes, replicas);
  }

  private static void mapReplicas(List<Replica> replicas, Function<Replica, Replica> mapper) {
    var iterator = replicas.listIterator();
    while (iterator.hasNext()) {
      var replica = iterator.next();
      iterator.set(mapper.apply(replica));
    }
  }

  private static Broker fakeNode(int brokerId) {
    var host = "fake-node-" + brokerId;
    var port = new Random(brokerId).nextInt(65535) + 1;
    var folders = Collections.synchronizedList(new ArrayList<Broker.DataFolder>());

    return FakeBroker.of(brokerId, host, port, folders);
  }

  interface FakeBroker extends Broker {

    static FakeBroker of(int id, String host, int port, List<DataFolder> folders) {
      var hashCode = Objects.hash(id, host, port);
      return new FakeBroker() {
        @Override
        public List<DataFolder> dataFolders() {
          return folders;
        }

        @Override
        public String host() {
          return host;
        }

        @Override
        public int port() {
          return port;
        }

        @Override
        public int id() {
          return id;
        }

        @Override
        public String toString() {
          return "FakeNodeInfo{" + "host=" + host() + ", id=" + id() + ", port=" + port() + '}';
        }

        @Override
        public int hashCode() {
          return hashCode;
        }

        @Override
        public boolean equals(Object other) {
          if (other instanceof NodeInfo) {
            var node = (NodeInfo) other;
            return id() == node.id() && port() == node.port() && host().equals(node.host());
          }
          return false;
        }
      };
    }

    @Override
    default boolean isController() {
      throw new UnsupportedOperationException();
    }

    @Override
    default Config config() {
      throw new UnsupportedOperationException();
    }

    @Override
    default Set<TopicPartition> topicPartitions() {
      throw new UnsupportedOperationException();
    }

    @Override
    default Set<TopicPartition> topicPartitionLeaders() {
      throw new UnsupportedOperationException();
    }
  }

  interface FakeDataFolder extends Broker.DataFolder {

    static FakeDataFolder of(String path) {
      return () -> path;
    }

    @Override
    default Map<TopicPartition, Long> partitionSizes() {
      throw new UnsupportedOperationException();
    }

    @Override
    default Map<TopicPartition, Long> orphanPartitionSizes() {
      throw new UnsupportedOperationException();
    }
  }
}
