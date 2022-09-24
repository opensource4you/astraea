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
package org.astraea.app.balancer;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.NodeInfo;
import org.astraea.common.admin.Replica;
import org.astraea.common.admin.TopicPartition;

public class FakeClusterInfo implements ClusterInfo<Replica> {

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
    final var nodes =
        IntStream.range(0, nodeCount)
            .mapToObj(nodeId -> NodeInfo.of(nodeId, "host" + nodeId, 9092))
            .collect(Collectors.toUnmodifiableList());
    final var dataDirCount = 3;
    final var dataDirectories =
        IntStream.range(0, dataDirCount)
            .mapToObj(i -> "/tmp/data-directory-" + i)
            .collect(Collectors.toUnmodifiableSet());
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
                                Replica.of(
                                    tp.topic(),
                                    tp.partition(),
                                    nodes.get(r),
                                    0,
                                    -1,
                                    r == 0,
                                    true,
                                    false,
                                    false,
                                    r == 0,
                                    dataDirectoryList.get(
                                        tp.partition() % dataDirectories.size()))))
            .collect(Collectors.toUnmodifiableList());

    return new FakeClusterInfo(
        new HashSet<>(nodes),
        nodes.stream()
            .collect(Collectors.toMap(NodeInfo::id, n -> new HashSet<String>(dataDirectories))),
        replicas);
  }

  private final Set<NodeInfo> nodes;
  private final Map<Integer, Set<String>> dataDirectories;
  private final List<Replica> replicas;

  FakeClusterInfo(
      Set<NodeInfo> nodes, Map<Integer, Set<String>> dataDirectories, List<Replica> replicas) {
    this.nodes = nodes;
    this.dataDirectories = dataDirectories;
    this.replicas = replicas;
  }

  @Override
  public Set<NodeInfo> nodes() {
    return nodes;
  }

  public Map<Integer, Set<String>> dataDirectories() {
    return dataDirectories;
  }

  @Override
  public Stream<Replica> replicaStream() {
    return replicas.stream();
  }
}
