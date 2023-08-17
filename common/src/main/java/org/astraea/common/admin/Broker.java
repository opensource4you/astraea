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
import java.util.Set;
import org.apache.kafka.clients.admin.LogDirDescription;

/**
 * @param id
 * @param host
 * @param port
 * @param isController
 * @param config config used by this node
 * @param dataFolders the disk folder used to stored data by this node
 */
public record Broker(
    int id,
    String host,
    int port,
    boolean isController,
    Config config,
    Set<String> dataFolders,
    List<TopicPartitionPath> topicPartitionPaths) {

  /**
   * @return true if the broker is offline. An offline node can't offer host or port information.
   */
  public boolean offline() {
    return host() == null || host().isEmpty() || port() < 0;
  }

  public static Broker of(int id, String host, int port) {
    return new Broker(id, host, port, false, Config.EMPTY, Set.of(), List.of());
  }

  public static Broker of(org.apache.kafka.common.Node node) {
    return of(node.id(), node.host(), node.port());
  }

  static Broker of(
      boolean isController,
      org.apache.kafka.common.Node nodeInfo,
      Map<String, String> configs,
      Map<String, LogDirDescription> dirs) {
    var config = new Config(configs);
    var paths = Set.copyOf(dirs.keySet());

    var topicPartitionPaths =
        dirs.entrySet().stream()
            .flatMap(
                entry ->
                    entry.getValue().replicaInfos().entrySet().stream()
                        .map(
                            tpEntry ->
                                new TopicPartitionPath(
                                    tpEntry.getKey().topic(),
                                    tpEntry.getKey().partition(),
                                    tpEntry.getValue().size(),
                                    entry.getKey())))
            .toList();
    return new Broker(
        nodeInfo.id(),
        nodeInfo.host(),
        nodeInfo.port(),
        isController,
        config,
        paths,
        topicPartitionPaths);
  }
}
