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
import java.util.stream.Collectors;

public interface Broker extends NodeInfo {

  static Broker of(
      boolean isController,
      org.astraea.common.admin.NodeInfo nodeInfo,
      org.apache.kafka.clients.admin.Config kafkaConfig,
      Map<String, org.apache.kafka.clients.admin.LogDirDescription> dirs) {
    var config = Config.of(kafkaConfig);
    var folders =
        dirs.entrySet().stream()
            .map(
                entry -> {
                  var path = entry.getKey();
                  var tpAndSize =
                      entry.getValue().replicaInfos().entrySet().stream()
                          .collect(
                              Collectors.toUnmodifiableMap(
                                  e -> TopicPartition.from(e.getKey()), e -> e.getValue().size()));
                  return (DataFolder)
                      new DataFolder() {

                        @Override
                        public String path() {
                          return path;
                        }

                        @Override
                        public Map<TopicPartition, Long> partitionSizes() {
                          return tpAndSize;
                        }
                      };
                })
            .collect(Collectors.toList());
    return new Broker() {
      @Override
      public String host() {
        return nodeInfo.host();
      }

      @Override
      public int port() {
        return nodeInfo.port();
      }

      @Override
      public int id() {
        return nodeInfo.id();
      }

      @Override
      public boolean isController() {
        return isController;
      }

      @Override
      public Config config() {
        return config;
      }

      @Override
      public List<DataFolder> folders() {
        return folders;
      }
    };
  }

  boolean isController();

  /** @return config used by this node */
  Config config();

  /** @return the disk folder used to stored data by this node */
  List<DataFolder> folders();

  interface DataFolder {

    /** @return the path on the local disk */
    String path();

    /** @return topic partition hosed by this node and size of files */
    Map<TopicPartition, Long> partitionSizes();
  }
}
