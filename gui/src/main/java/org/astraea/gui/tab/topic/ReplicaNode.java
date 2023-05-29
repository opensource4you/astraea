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
package org.astraea.gui.tab.topic;

import java.time.Duration;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javafx.scene.Node;
import org.astraea.common.DataSize;
import org.astraea.common.MapUtils;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.TopicPartition;
import org.astraea.common.admin.TopicPartitionReplica;
import org.astraea.common.function.Bi3Function;
import org.astraea.gui.Context;
import org.astraea.gui.Logger;
import org.astraea.gui.pane.Argument;
import org.astraea.gui.pane.FirstPart;
import org.astraea.gui.pane.PaneBuilder;
import org.astraea.gui.pane.SecondPart;
import org.astraea.gui.text.EditableText;
import org.astraea.gui.text.TextInput;

class ReplicaNode {

  static final String TOPIC_NAME_KEY = "topic";
  static final String PARTITION_KEY = "partition";
  static final String PATH_KEY = "path";
  static final String LEADER_SIZE_KEY = "leader size";
  static final String PROGRESS_KEY = "progress";

  static final String MOVE_BROKER_KEY = "move to brokers";

  static List<Map<String, Object>> allResult(ClusterInfo clusterInfo) {
    // There are two leader replicas if the leader replica is moving to another folder
    var leaderSizes = ClusterInfo.leaderSize(clusterInfo);
    return clusterInfo
        .replicaStream()
        .map(
            replica -> {
              var leaderSize = leaderSizes.getOrDefault(replica.topicPartition(), 0L);
              var result = new LinkedHashMap<String, Object>();
              result.put(TOPIC_NAME_KEY, replica.topic());
              result.put(PARTITION_KEY, replica.partition());
              result.put("internal", replica.isInternal());
              result.put("broker", replica.brokerId());
              if (replica.path() != null) result.put(PATH_KEY, replica.path());
              result.put("isLeader", replica.isLeader());
              result.put("isPreferredLeader", replica.isPreferredLeader());
              result.put("isOffline", replica.isOffline());
              result.put("isFuture", replica.isFuture());
              result.put("inSync", replica.isSync());
              result.put("isAdding", replica.isAdding());
              result.put("isRemoving", replica.isRemoving());
              result.put("lag", replica.lag());
              result.put("size", DataSize.Byte.of(replica.size()));
              if (leaderSize > replica.size()) {
                result.put(LEADER_SIZE_KEY, DataSize.Byte.of(leaderSize));
                result.put(
                    PROGRESS_KEY,
                    String.format(
                        "%.2f%%",
                        leaderSize == 0
                            ? 100D
                            : ((double) replica.size() / (double) leaderSize) * 100));
              }
              return result;
            })
        .collect(Collectors.toList());
  }

  static Bi3Function<List<Map<String, Object>>, Argument, Logger, CompletionStage<Void>>
      tableViewAction(Context context) {
    return (items, inputs, logger) -> {
      var partitions =
          items.stream()
              .flatMap(
                  item -> {
                    var topic = item.get(TOPIC_NAME_KEY);
                    var partition = item.get(PARTITION_KEY);
                    if (topic != null && partition != null)
                      return Stream.of(
                          TopicPartition.of(
                              topic.toString(), Integer.parseInt(partition.toString())));
                    return Stream.of();
                  })
              .collect(Collectors.toSet());
      if (partitions.isEmpty()) {
        logger.log("nothing to alert");
        return CompletableFuture.completedStage(null);
      }
      var moveTo =
          inputs
              .get(MOVE_BROKER_KEY)
              .map(
                  s ->
                      Arrays.stream(s.split(","))
                          .map(
                              idAndPath -> {
                                var ss = idAndPath.split(":");
                                if (ss.length == 1)
                                  return Map.entry(
                                      Integer.parseInt(ss[0]), Optional.<String>empty());
                                else return Map.entry(Integer.parseInt(ss[0]), Optional.of(ss[1]));
                              })
                          .collect(
                              MapUtils.toLinkedHashMap(Map.Entry::getKey, Map.Entry::getValue)));
      if (moveTo.isEmpty()) {
        logger.log("please define " + MOVE_BROKER_KEY);
        return CompletableFuture.completedStage(null);
      }

      var requestToMoveBrokers =
          partitions.stream()
              .collect(Collectors.toMap(tp -> tp, tp -> List.copyOf(moveTo.get().keySet())));

      var requestToMoveFolders =
          requestToMoveBrokers.entrySet().stream()
              .flatMap(
                  entry ->
                      entry.getValue().stream()
                          .flatMap(
                              id ->
                                  moveTo
                                      .get()
                                      .getOrDefault(id, Optional.empty())
                                      .map(
                                          path ->
                                              Map.entry(
                                                  TopicPartitionReplica.of(
                                                      entry.getKey().topic(),
                                                      entry.getKey().partition(),
                                                      id),
                                                  path))
                                      .stream()))
              .collect(MapUtils.toLinkedHashMap(Map.Entry::getKey, Map.Entry::getValue));

      return context
          .admin()
          .internalTopicNames()
          .thenCompose(
              internalTopics -> {
                var internal =
                    partitions.stream()
                        .map(TopicPartition::topic)
                        .filter(internalTopics::contains)
                        .collect(Collectors.toSet());
                if (!internal.isEmpty()) {
                  logger.log("internal topics: " + internal + " can't be altered");
                  return CompletableFuture.completedStage(null);
                }

                return context
                    .admin()
                    .moveToBrokers(
                        moveTo
                            .map(
                                bkAndPaths ->
                                    partitions.stream()
                                        .collect(
                                            Collectors.toMap(
                                                tp -> tp, tp -> List.copyOf(bkAndPaths.keySet()))))
                            .orElse(Map.of()))
                    .thenCompose(
                        ignored ->
                            context
                                .admin()
                                .waitCluster(
                                    partitions.stream()
                                        .map(TopicPartition::topic)
                                        .collect(Collectors.toSet()),
                                    rs ->
                                        requestToMoveBrokers.entrySet().stream()
                                            .allMatch(
                                                entry ->
                                                    rs.replicas(entry.getKey()).stream()
                                                        .map(r -> r.brokerId())
                                                        .collect(Collectors.toSet())
                                                        .containsAll(entry.getValue())),
                                    Duration.ofSeconds(10),
                                    1))
                    .thenCompose(ignored -> context.admin().moveToFolders(requestToMoveFolders))
                    .thenAccept(
                        ignored -> logger.log("succeed to alter partitions: " + partitions));
              });
    };
  }

  static Node of(Context context) {
    var firstPart =
        FirstPart.builder()
            .clickName("REFRESH")
            .tableRefresher(
                (argument, logger) ->
                    context
                        .admin()
                        .topicNames(true)
                        .thenCompose(context.admin()::clusterInfo)
                        .thenApply(ReplicaNode::allResult))
            .build();
    var secondPart =
        SecondPart.builder()
            .buttonName("ALTER")
            .textInputs(
                List.of(
                    TextInput.of(
                        MOVE_BROKER_KEY,
                        EditableText.multiline().disable().hint("1001:/path,1002").build())))
            .action(tableViewAction(context))
            .build();
    return PaneBuilder.of().firstPart(firstPart).secondPart(secondPart).build();
  }
}
