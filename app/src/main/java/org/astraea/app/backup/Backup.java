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
package org.astraea.app.backup;

import java.util.Comparator;
import java.util.stream.Collectors;
import org.astraea.common.admin.Admin;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.Replica;

public class Backup {
  public static void main(String[] args) {}

  public void restoreDistribution(ClusterInfo clusterInfo, String bootstrapServers) {
    try (var admin = Admin.of(bootstrapServers)) {
      clusterInfo
          .topics()
          .values()
          .forEach(
              topic ->
                  admin
                      .creator()
                      .topic(topic.name())
                      .replicasAssignments(
                          clusterInfo
                              .replicaStream(topic.name())
                              .collect(
                                  Collectors.groupingBy(
                                      Replica::partition,
                                      Collectors.mapping(
                                          replica -> replica,
                                          Collectors.collectingAndThen(
                                              Collectors.toList(),
                                              list ->
                                                  list.stream()
                                                      .sorted(
                                                          Comparator.comparing(
                                                              replica -> !replica.isLeader()))
                                                      .map(replica -> replica.nodeInfo().id())
                                                      .collect(Collectors.toUnmodifiableList()))))))
                      .configs(topic.config().raw())
                      .run()
                      .toCompletableFuture()
                      .join());
    }
  }
}
