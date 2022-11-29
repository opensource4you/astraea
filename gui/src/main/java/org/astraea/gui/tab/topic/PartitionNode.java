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

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javafx.scene.Node;
import org.astraea.common.FutureUtils;
import org.astraea.common.admin.Partition;
import org.astraea.common.admin.TopicPartition;
import org.astraea.common.function.Bi3Function;
import org.astraea.gui.Context;
import org.astraea.gui.Logger;
import org.astraea.gui.pane.Argument;
import org.astraea.gui.pane.MultiInput;
import org.astraea.gui.pane.PaneBuilder;
import org.astraea.gui.text.EditableText;
import org.astraea.gui.text.TextInput;

public class PartitionNode {

  private static final String TOPIC_NAME_KEY = "topic";
  private static final String PARTITION_KEY = "partition";

  private static final String INCREASE_PARTITION_KEY = "increase partitions to";

  private static final String TRUNCATE_OFFSET_KEY = "truncate offset to";

  private static List<Map<String, Object>> basicResult(List<Partition> ps) {
    return ps.stream()
        .sorted(Comparator.comparing(Partition::topic).thenComparing(Partition::partition))
        .map(
            p -> {
              var result = new LinkedHashMap<String, Object>();
              result.put(TOPIC_NAME_KEY, p.topic());
              result.put(PARTITION_KEY, p.partition());
              result.put("internal", p.internal());
              p.leader().ifPresent(l -> result.put("leader", l.id()));
              result.put(
                  "replicas",
                  p.replicas().stream()
                      .map(n -> String.valueOf(n.id()))
                      .collect(Collectors.joining(",")));
              result.put(
                  "isr",
                  p.isr().stream()
                      .map(n -> String.valueOf(n.id()))
                      .collect(Collectors.joining(",")));
              result.put("earliest offset", p.earliestOffset());
              result.put("latest offset", p.latestOffset());
              p.maxTimestamp()
                  .ifPresent(
                      t ->
                          result.put(
                              "max timestamp",
                              LocalDateTime.ofInstant(
                                  Instant.ofEpochMilli(t), ZoneId.systemDefault())));
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
      var increasePartitions = inputs.get(INCREASE_PARTITION_KEY).map(Integer::parseInt);
      var offset = inputs.get(TRUNCATE_OFFSET_KEY).map(Long::parseLong);
      if (increasePartitions.isEmpty() && offset.isEmpty()) {
        logger.log("please define either " + INCREASE_PARTITION_KEY + " or " + TRUNCATE_OFFSET_KEY);
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
                return FutureUtils.combine(
                    context
                        .admin()
                        .deleteRecords(
                            offset
                                .map(
                                    o ->
                                        partitions.stream()
                                            .collect(Collectors.toMap(tp -> tp, tp -> o)))
                                .orElse(Map.of())),
                    increasePartitions
                        .map(
                            total ->
                                FutureUtils.sequence(
                                    partitions.stream()
                                        .map(TopicPartition::topic)
                                        .distinct()
                                        .map(
                                            t ->
                                                context
                                                    .admin()
                                                    .addPartitions(t, total)
                                                    .toCompletableFuture())
                                        .collect(Collectors.toList())))
                        .orElse(CompletableFuture.completedFuture(List.of())),
                    (i, j) -> {
                      logger.log("succeed to alter partitions: " + partitions);
                      return null;
                    });
              });
    };
  }

  static Node of(Context context) {
    var moveToKey = "move to brokers";
    var offsetKey = "truncate to offset";
    return PaneBuilder.of()
        .secondPart(
            MultiInput.of(
                List.of(
                    TextInput.of(
                        INCREASE_PARTITION_KEY, EditableText.singleLine().disable().build()),
                    TextInput.of(
                        TRUNCATE_OFFSET_KEY, EditableText.singleLine().disable().build()))),
            "ALTER",
            tableViewAction(context))
        .firstPart(
            "REFRESH",
            (argument, logger) ->
                context
                    .admin()
                    .topicNames(true)
                    .thenCompose(context.admin()::partitions)
                    .thenApply(PartitionNode::basicResult))
        .build();
  }
}
