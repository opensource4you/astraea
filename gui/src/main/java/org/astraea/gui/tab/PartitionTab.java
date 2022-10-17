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
package org.astraea.gui.tab;

import java.time.Duration;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;
import javafx.geometry.Side;
import javafx.scene.layout.Pane;
import org.astraea.common.LinkedHashMap;
import org.astraea.common.Utils;
import org.astraea.common.admin.NodeInfo;
import org.astraea.common.admin.Partition;
import org.astraea.common.admin.TopicPartition;
import org.astraea.gui.Context;
import org.astraea.gui.pane.BorderPane;
import org.astraea.gui.pane.PaneBuilder;
import org.astraea.gui.pane.Tab;
import org.astraea.gui.pane.TabPane;

public class PartitionTab {

  static List<Map<String, Object>> basicResult(List<Partition> ps) {
    return ps.stream()
        .sorted(Comparator.comparing(Partition::topic).thenComparing(Partition::partition))
        .map(
            p ->
                LinkedHashMap.<String, Object>of(
                    "topic",
                    p.topic(),
                    "partition",
                    p.partition(),
                    "leader",
                    p.leader().map(NodeInfo::id).orElse(-1),
                    "replicas",
                    p.replicas().stream()
                        .map(n -> String.valueOf(n.id()))
                        .collect(Collectors.joining(",")),
                    "isr",
                    p.isr().stream()
                        .map(n -> String.valueOf(n.id()))
                        .collect(Collectors.joining(",")),
                    "earliest offset",
                    p.earliestOffset(),
                    "latest offset",
                    p.latestOffset(),
                    "max timestamp",
                    Utils.format(p.maxTimestamp())))
        .collect(Collectors.toList());
  }

  private static Tab basicTab(Context context) {
    return Tab.of(
        "partition",
        PaneBuilder.of()
            .searchField("topic name")
            .buttonAction(
                (input, logger) ->
                    context
                        .admin()
                        .topicNames(true)
                        .thenApply(
                            names ->
                                names.stream()
                                    .filter(input::matchSearch)
                                    .collect(Collectors.toSet()))
                        .thenCompose(context.admin()::partitions)
                        .thenApply(PartitionTab::basicResult))
            .build());
  }

  private static Pane alterPane(Context context, String topic, List<Partition> partitions) {
    var partitionsKey = "partition ids";
    var all = "all";
    var moveToKey = "move to brokers";
    var offsetKey = "truncate to offset";
    return PaneBuilder.of()
        .buttonName("ALTER")
        .input(partitionsKey, false, false, true, all)
        .input(moveToKey, false, false)
        .input(offsetKey, false, true)
        .initTableView(basicResult(partitions))
        .buttonAction(
            (input, logger) ->
                // delay to fetch partitions
                CompletableFuture.runAsync(() -> Utils.sleep(Duration.ofSeconds(2)))
                    .thenCompose(ignored -> context.admin().partitions(Set.of(topic)))
                    .thenApply(PartitionTab::basicResult))
        .buttonListener(
            (input, logger) -> {
              if (!input.nonEmptyTexts().containsKey(moveToKey)
                  && !input.nonEmptyTexts().containsKey(moveToKey)) {
                logger.log("Please define either \"move to\" or \"offset\"");
                return CompletableFuture.completedFuture(null);
              }

              return Optional.ofNullable(input.nonEmptyTexts().get(partitionsKey))
                  .filter(tpString -> !tpString.equals(all))
                  .map(
                      tpString ->
                          CompletableFuture.completedStage(
                              Arrays.stream(tpString.split(","))
                                  .map(Integer::parseInt)
                                  .map(id -> TopicPartition.of(topic, id))
                                  .collect(Collectors.toUnmodifiableSet())))
                  .orElseGet(() -> context.admin().topicPartitions(Set.of(topic)))
                  .thenApply(
                      tps ->
                          Map.entry(
                              Optional.ofNullable(input.nonEmptyTexts().get(offsetKey))
                                  .map(Long::parseLong)
                                  .map(
                                      offset ->
                                          tps.stream()
                                              .collect(
                                                  Collectors.toMap(
                                                      Function.identity(), ignored -> offset)))
                                  .orElse(Map.of()),
                              Optional.ofNullable(input.nonEmptyTexts().get(moveToKey))
                                  .map(
                                      s ->
                                          Arrays.stream(s.split(","))
                                              .map(Integer::parseInt)
                                              .collect(Collectors.toList()))
                                  .map(
                                      m ->
                                          tps.stream()
                                              .collect(
                                                  Collectors.toMap(
                                                      Function.identity(), ignored -> m)))
                                  .orElse(Map.of())))
                  .thenCompose(
                      entry ->
                          context
                              .admin()
                              .deleteRecords(entry.getKey())
                              .thenCompose(
                                  ignored -> context.admin().moveToBrokers(entry.getValue()))
                              .thenAccept(
                                  ignored ->
                                      logger.log(
                                          "succeed to alter "
                                              + (entry.getKey().keySet().isEmpty()
                                                  ? entry.getValue().keySet()
                                                  : entry.getKey().keySet()))));
            })
        .build();
  }

  public static Tab alterTab(Context context) {
    return Tab.dynamic(
        "alter",
        () ->
            context
                .admin()
                .topicNames(false)
                .thenCompose(context.admin()::partitions)
                .thenApply(
                    partitions ->
                        partitions.stream().collect(Collectors.groupingBy(Partition::topic)))
                .thenApply(
                    topicAndPartitions ->
                        BorderPane.selectableTop(
                            topicAndPartitions.entrySet().stream()
                                .collect(
                                    Utils.toSortedMap(
                                        Map.Entry::getKey,
                                        e -> alterPane(context, e.getKey(), e.getValue()))))));
  }

  public static Tab of(Context context) {
    return Tab.of("partition", TabPane.of(Side.TOP, List.of(basicTab(context), alterTab(context))));
  }
}
