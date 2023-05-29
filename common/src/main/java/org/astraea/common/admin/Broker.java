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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.LogDirDescription;

/**
 * @param id
 * @param host
 * @param port
 * @param isController
 * @param config config used by this node
 * @param dataFolders the disk folder used to stored data by this node
 * @param topicPartitions
 * @param topicPartitionLeaders partition leaders hosted by this broker
 */
public record Broker(
    int id,
    String host,
    int port,
    boolean isController,
    Config config,
    List<DataFolder> dataFolders,
    Set<TopicPartition> topicPartitions,
    Set<TopicPartition> topicPartitionLeaders) {

  /**
   * @return true if the broker is offline. An offline node can't offer host or port information.
   */
  public boolean offline() {
    return host() == null || host().isEmpty() || port() < 0;
  }

  public static Broker of(int id, String host, int port) {
    return new Broker(id, host, port, false, Config.EMPTY, List.of(), Set.of(), Set.of());
  }

  public static Broker of(org.apache.kafka.common.Node node) {
    return of(node.id(), node.host(), node.port());
  }

  static Broker of(
      boolean isController,
      org.apache.kafka.common.Node nodeInfo,
      Map<String, String> configs,
      Map<String, LogDirDescription> dirs,
      Collection<org.apache.kafka.clients.admin.TopicDescription> topics) {
    var config = new Config(configs);
    var partitionsFromTopicDesc =
        topics.stream()
            .flatMap(
                t ->
                    t.partitions().stream()
                        .filter(p -> p.replicas().stream().anyMatch(n -> n.id() == nodeInfo.id()))
                        .map(p -> TopicPartition.of(t.name(), p.partition())))
            .collect(Collectors.toUnmodifiableSet());
    var folders =
        dirs.entrySet().stream()
            .map(
                entry -> {
                  var path = entry.getKey();
                  var allPartitionAndSize =
                      entry.getValue().replicaInfos().entrySet().stream()
                          .collect(
                              Collectors.toUnmodifiableMap(
                                  e -> TopicPartition.from(e.getKey()), e -> e.getValue().size()));
                  var partitionSizes =
                      allPartitionAndSize.entrySet().stream()
                          .filter(tpAndSize -> partitionsFromTopicDesc.contains(tpAndSize.getKey()))
                          .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                  var orphanPartitionSizes =
                      allPartitionAndSize.entrySet().stream()
                          .filter(
                              tpAndSize -> !partitionsFromTopicDesc.contains(tpAndSize.getKey()))
                          .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

                  return new DataFolder(path, partitionSizes, orphanPartitionSizes);
                })
            .toList();
    var topicPartitionLeaders =
        topics.stream()
            .flatMap(
                topic ->
                    topic.partitions().stream()
                        .filter(p -> p.leader() != null && p.leader().id() == nodeInfo.id())
                        .map(p -> TopicPartition.of(topic.name(), p.partition())))
            .collect(Collectors.toUnmodifiableSet());
    return new Broker(
        nodeInfo.id(),
        nodeInfo.host(),
        nodeInfo.port(),
        isController,
        config,
        folders,
        partitionsFromTopicDesc,
        topicPartitionLeaders);
  }

  /**
   * @param path the path on the local disk
   * @param partitionSizes topic partition hosed by this node and size of files
   * @param orphanPartitionSizes topic partition located by this node but not traced by cluster
   */
  public record DataFolder(
      String path,
      Map<TopicPartition, Long> partitionSizes,
      Map<TopicPartition, Long> orphanPartitionSizes) {}
}
