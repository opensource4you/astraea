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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/** A builder with utility functions for modifying a {@link ClusterInfo}. */
public class ClusterInfoBuilder {

  private final ClusterInfo<Replica> sourceCluster;
  private final List<BiConsumer<Set<NodeInfo>, List<Replica>>> alterations;

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
   * Add an alteration to the cluster state.
   *
   * @param alteration the alteration that will be applied to the cluster state, all the given data
   *     structure are mutable but not thread-safe.
   * @return this.
   */
  public ClusterInfoBuilder apply(BiConsumer<Set<NodeInfo>, List<Replica>> alteration) {
    this.alterations.add(alteration);
    return this;
  }

  /**
   * Add fake brokers into the cluster state.
   *
   * @param brokerIds the id of fake brokers.
   * @return this.
   */
  public ClusterInfoBuilder addNode(Set<Integer> brokerIds) {
    return apply(
        (nodes, replicas) ->
            brokerIds.stream().map(ClusterInfoBuilder::fakeNode).forEach(nodes::add));
  }

  /**
   * Add some fake folders to a specific broker.
   *
   * @param folders the path of fake folders associated with each broker.
   * @return this.
   */
  public ClusterInfoBuilder addFolders(Map<Integer, Set<String>> folders) {
    return apply(
        (nodes, replicas) ->
            folders.forEach(
                (brokerId, newFolders) -> {
                  var theNode =
                      nodes.stream()
                          .filter(node -> node.id() == brokerId)
                          .findFirst()
                          .orElseThrow(
                              () ->
                                  new IllegalArgumentException(
                                      "Looking for the node but no match found: " + brokerId));

                  if (theNode instanceof FakeBroker)
                    newFolders.forEach(folder -> addFolder((FakeBroker) theNode, folder));
                  else throw new IllegalStateException("No support for mocking a concrete node");
                }));
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
    return apply(
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
              .map(mapper)
              .forEach(replicas::add);
        });
  }

  /**
   * Apply alteration to specific replicas.
   *
   * @param mapper modification applied to the matched replica.
   * @return this.
   */
  public ClusterInfoBuilder mapLog(Function<Replica, Replica> mapper) {
    return apply((nodes, replicas) -> mapReplicas(replicas, mapper));
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
    return apply(
        (nodes, replicas) -> {
          var newNode =
              nodes.stream()
                  .filter(n -> n.id() == toBroker)
                  .findFirst()
                  .orElseThrow(() -> new IllegalArgumentException("No such replica: " + toBroker));
          var matched = new AtomicBoolean(false);
          mapReplicas(
              replicas,
              r -> {
                if (r.topicPartitionReplica().equals(replica)) {
                  matched.set(true);
                  return Replica.builder(r).nodeInfo(newNode).path(toDir).build();
                } else {
                  return r;
                }
              });
          if (!matched.get()) throw new IllegalArgumentException("No such replica: " + replica);
        });
  }

  /**
   * Let the specific replica become the preferred leader of its partition.
   *
   * @param replica the replica to become preferred leader.
   * @return this.
   */
  public ClusterInfoBuilder setPreferredLeader(TopicPartitionReplica replica) {
    return apply(
        (nodes, replicas) -> {
          var matched = new AtomicBoolean(false);

          mapReplicas(
              replicas,
              (r) -> {
                if (r.topicPartitionReplica().equals(replica)) {
                  matched.set(true);
                  return Replica.builder(r).isLeader(true).isPreferredLeader(true).build();
                } else if (r.topicPartition().equals(replica.topicPartition())
                    && (r.isPreferredLeader() || r.isLeader())) {
                  return Replica.builder(r).isLeader(false).isPreferredLeader(false).build();
                } else {
                  return r;
                }
              });

          if (!matched.get()) throw new IllegalArgumentException("No such replica: " + replica);
        });
  }

  /**
   * Apply the pending alteration to the source cluster, and return the transformed {@link
   * ClusterInfo}.
   */
  public ClusterInfo<Replica> build() {
    final var nodes = new HashSet<>(sourceCluster.nodes());
    final var replicas = new ArrayList<>(sourceCluster.replicas());
    alterations.forEach(alteration -> alteration.accept(nodes, replicas));

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

  private static void addFolder(FakeBroker fakeBroker, String path) {
    fakeBroker.dataFolders().add((FakeDataFolder) () -> path);
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
