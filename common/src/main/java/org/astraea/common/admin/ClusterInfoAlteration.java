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
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/** A functional interface to transform a given {@link ClusterInfo} with specific logic */
@FunctionalInterface
public interface ClusterInfoAlteration<T extends ReplicaInfo> {

  /**
   * Alter the given source ClusterInfo with specific logic
   *
   * @param nodes current nodes in cluster, this collection is mutable.
   * @param replicas current replicas in the cluster, this collection is mutable.
   */
  void apply(Set<NodeInfo> nodes, List<T> replicas);

  static <T extends ReplicaInfo> ClusterInfoAlteration<T> addNode(Integer... brokerIds) {
    return addNode(Arrays.asList(brokerIds));
  }

  static <T extends ReplicaInfo> ClusterInfoAlteration<T> addNode(List<Integer> brokerIds) {
    return (nodes, replicas) ->
        brokerIds.stream().map(ClusterInfoAlteration::fakeNode).forEach(nodes::add);
  }

  static <T extends ReplicaInfo> ClusterInfoAlteration<T> addFolders(
      int brokerId, String... folders) {
    return addFolders(Map.of(brokerId, Set.copyOf(Arrays.asList(folders))));
  }

  static <T extends ReplicaInfo> ClusterInfoAlteration<T> addFolders(
      Map<Integer, Set<String>> folders) {
    return (nodes, replicas) ->
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
            });
  }

  static ClusterInfoAlteration<Replica> addTopic(
      String topicName, int partitionSize, short replicaFactor) {
    return addTopic(topicName, partitionSize, replicaFactor, x -> x);
  }

  static ClusterInfoAlteration<Replica> addTopic(
      String topicName, int partitionSize, short replicaFactor, Function<Replica, Replica> mapper) {
    return (nodes, replicas) -> {
      if (nodes.stream().anyMatch(node -> !(node instanceof Broker)))
        throw new IllegalStateException("All the nodes must include the folder info");
      if (nodes.size() < replicaFactor)
        throw new IllegalArgumentException(
            "Insufficient node for this replica factor: " + nodes.size() + " < " + replicaFactor);
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

      IntStream.range(0, partitionSize)
          .mapToObj(partition -> TopicPartition.of(topicName, partition))
          .flatMap(
              tp ->
                  IntStream.range(0, replicaFactor)
                      .mapToObj(
                          index ->
                              new FakeReplica() {
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
                                final boolean leader = index == 0;

                                @Override
                                public boolean isPreferredLeader() {
                                  return leader;
                                }

                                @Override
                                public String path() {
                                  return path;
                                }

                                @Override
                                public String topic() {
                                  return tp.topic();
                                }

                                @Override
                                public int partition() {
                                  return tp.partition();
                                }

                                @Override
                                public NodeInfo nodeInfo() {
                                  return broker;
                                }

                                @Override
                                public boolean isLeader() {
                                  return leader;
                                }

                                @Override
                                public String toString() {
                                  return Replica.toString(this);
                                }
                              }))
          .map(mapper)
          .forEach(replicas::add);
    };
  }

  static ClusterInfoAlteration<Replica> mapLog(
      Predicate<Replica> filter, Function<Replica, Replica> mapper) {
    return (nodes, replicas) -> {
      var iterator = replicas.listIterator();
      while (iterator.hasNext()) {
        var replica = iterator.next();
        if (filter.test(replica)) iterator.set(mapper.apply(replica));
      }
    };
  }

  static ClusterInfoAlteration<Replica> mapTopic(String topic, Function<Replica, Replica> mapper) {
    return mapLog(replica -> replica.topic().equals(topic), mapper);
  }

  static ClusterInfoAlteration<Replica> mapTopicPartition(
      TopicPartition topicPartition, Function<Replica, Replica> mapper) {
    return mapLog(replica -> topicPartition.equals(replica.topicPartition()), mapper);
  }

  private static Broker fakeNode(int brokerId) {
    var host = "fake-node-" + brokerId;
    var port = ThreadLocalRandom.current().nextInt(1024, 65536);
    var folders = Collections.synchronizedList(new ArrayList<Broker.DataFolder>());

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
        return brokerId;
      }

      @Override
      public String toString() {
        return "FakeNodeInfo{" + "host=" + host() + ", id=" + id() + ", port=" + port() + '}';
      }
    };
  }

  private static void addFolder(FakeBroker fakeBroker, String path) {
    fakeBroker.dataFolders().add((FakeDataFolder) () -> path);
  }

  interface FakeBroker extends Broker {
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

  interface FakeReplica extends Replica {

    @Override
    default boolean isFuture() {
      return false;
    }

    @Override
    default long lag() {
      return 0;
    }

    @Override
    default long size() {
      return 0;
    }

    @Override
    default boolean internal() {
      return false;
    }

    @Override
    default boolean inSync() {
      return true;
    }

    @Override
    default boolean isOffline() {
      return false;
    }

    @Override
    default boolean isAdding() {
      return false;
    }

    @Override
    default boolean isRemoving() {
      return false;
    }
  }
}
