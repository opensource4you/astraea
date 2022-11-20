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
package org.astraea.common.balancer;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.astraea.common.admin.Broker;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.Config;
import org.astraea.common.admin.NodeInfo;
import org.astraea.common.admin.Replica;
import org.astraea.common.admin.TopicPartition;

public class FakeClusterInfo extends ClusterInfo.Optimized<Replica> {

  public static FakeClusterInfo of(
      int nodeCount, int topicCount, int partitionCount, int replicaCount) {
    final var random = new Random();
    random.setSeed(0);

    return of(
        nodeCount,
        topicCount,
        partitionCount,
        replicaCount,
        (partition) ->
            IntStream.range(0, partition)
                .mapToObj(
                    (ignore) -> {
                      final var suffix =
                          IntStream.range(0, 5)
                              .map(i -> random.nextInt(26))
                              .map(i -> 'a' + i)
                              .mapToObj(i -> String.valueOf((char) i))
                              .collect(Collectors.joining());
                      return "fake-topic-" + suffix;
                    })
                .collect(Collectors.toUnmodifiableSet()));
  }

  public static FakeClusterInfo of(
      int nodeCount,
      int topicCount,
      int partitionCount,
      int replicaCount,
      Function<Integer, Set<String>> topicNameGenerator) {
    final var dataDirCount = 3;
    final var dataDirectories =
        IntStream.range(0, dataDirCount)
            .mapToObj(i -> "/tmp/data-directory-" + i)
            .collect(Collectors.toUnmodifiableSet());
    final var nodes =
        IntStream.range(0, nodeCount)
            .mapToObj(nodeId -> NodeInfo.of(nodeId, "host" + nodeId, 9092))
            .map(
                node ->
                    new Broker() {
                      @Override
                      public boolean isController() {
                        throw new UnsupportedOperationException();
                      }

                      @Override
                      public Config config() {
                        throw new UnsupportedOperationException();
                      }

                      @Override
                      public List<DataFolder> dataFolders() {
                        return dataDirectories.stream()
                            .map(
                                x ->
                                    new DataFolder() {
                                      @Override
                                      public String path() {
                                        return x;
                                      }

                                      @Override
                                      public Map<TopicPartition, Long> partitionSizes() {
                                        throw new UnsupportedOperationException();
                                      }

                                      @Override
                                      public Map<TopicPartition, Long> orphanPartitionSizes() {
                                        throw new UnsupportedOperationException();
                                      }
                                    })
                            .collect(Collectors.toUnmodifiableList());
                      }

                      @Override
                      public Set<TopicPartition> topicPartitions() {
                        throw new UnsupportedOperationException();
                      }

                      @Override
                      public Set<TopicPartition> topicPartitionLeaders() {
                        throw new UnsupportedOperationException();
                      }

                      @Override
                      public String host() {
                        return node.host();
                      }

                      @Override
                      public int port() {
                        return node.port();
                      }

                      @Override
                      public int id() {
                        return node.id();
                      }
                    })
            .collect(Collectors.toUnmodifiableList());
    final var dataDirectoryList = List.copyOf(dataDirectories);
    final var topics = topicNameGenerator.apply(topicCount);
    final var replicas =
        topics.stream()
            .flatMap(
                topic ->
                    IntStream.range(0, partitionCount)
                        .mapToObj(p -> TopicPartition.of(topic, Integer.toString(p))))
            .flatMap(
                tp ->
                    IntStream.range(0, replicaCount)
                        .mapToObj(
                            r ->
                                Replica.builder()
                                    .topic(tp.topic())
                                    .partition(tp.partition())
                                    .nodeInfo(nodes.get(r))
                                    .lag(0)
                                    .size(-1)
                                    .isLeader(r == 0)
                                    .inSync(true)
                                    .isFuture(false)
                                    .isOffline(false)
                                    .isPreferredLeader(r == 0)
                                    .path(
                                        dataDirectoryList.get(
                                            tp.partition() % dataDirectories.size()))
                                    .build()))
            .collect(Collectors.toUnmodifiableList());

    return new FakeClusterInfo(new HashSet<>(nodes), replicas);
  }

  FakeClusterInfo(Set<NodeInfo> nodes, List<Replica> replicas) {
    super(nodes, replicas);
  }
}
