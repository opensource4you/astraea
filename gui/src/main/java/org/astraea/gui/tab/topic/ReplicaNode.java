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

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javafx.scene.Node;
import org.astraea.common.DataSize;
import org.astraea.common.admin.Replica;
import org.astraea.common.admin.ReplicaInfo;
import org.astraea.common.admin.TopicPartition;
import org.astraea.common.function.Bi3Function;
import org.astraea.gui.Context;
import org.astraea.gui.Logger;
import org.astraea.gui.pane.Input;
import org.astraea.gui.pane.Lattice;
import org.astraea.gui.pane.PaneBuilder;
import org.astraea.gui.text.EditableText;
import org.astraea.gui.text.TextInput;

public class ReplicaNode {

  static final String TOPIC_NAME_KEY = "topic";
  static final String PARTITION_KEY = "partition";
  static final String PATH_KEY = "path";
  static final String LEADER_SIZE_KEY = "leader size";
  static final String PROGRESS_KEY = "progress";

  static final String MOVE_BROKER_KEY = "move to brokers";

  static List<Map<String, Object>> allResult(List<Replica> replicas) {
    var leaderSizes =
        replicas.stream()
            .filter(ReplicaInfo::isLeader)
            .collect(Collectors.toMap(ReplicaInfo::topicPartition, Replica::size));
    return replicas.stream()
        .map(
            replica -> {
              var leaderSize = leaderSizes.getOrDefault(replica.topicPartition(), 0L);
              var result = new LinkedHashMap<String, Object>();
              result.put(TOPIC_NAME_KEY, replica.topic());
              result.put(PARTITION_KEY, replica.partition());
              result.put("broker", replica.nodeInfo().id());
              if (replica.path() != null) result.put(PATH_KEY, replica.path());
              result.put("isLeader", replica.isLeader());
              result.put("isPreferredLeader", replica.isPreferredLeader());
              result.put("isOffline", replica.isOffline());
              result.put("isFuture", replica.isFuture());
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

  static Bi3Function<List<Map<String, Object>>, Input, Logger, CompletionStage<Void>>
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
                          .map(Integer::parseInt)
                          .collect(Collectors.toList()));
      if (moveTo.isEmpty()) {
        logger.log("please define " + MOVE_BROKER_KEY);
        return CompletableFuture.completedStage(null);
      }

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
                                bks ->
                                    partitions.stream()
                                        .collect(Collectors.toMap(tp -> tp, tp -> bks)))
                            .orElse(Map.of()))
                    .thenAccept(
                        ignored -> {
                          logger.log("succeed to alter partitions: " + partitions);
                        });
              });
    };
  }

  static Node of(Context context) {
    return PaneBuilder.of()
        .tableRefresher(
            (input, logger) ->
                context
                    .admin()
                    .topicNames(true)
                    .thenCompose(context.admin()::replicas)
                    .thenApply(ReplicaNode::allResult))
        .tableViewAction(
            Lattice.of(
                List.of(
                    TextInput.of(
                        MOVE_BROKER_KEY,
                        EditableText.singleLine().disable().hint("1001,1002").build()))),
            "ALTER",
            tableViewAction(context))
        .build();
  }
}
