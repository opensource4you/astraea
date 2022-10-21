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
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;
import javafx.geometry.Side;
import org.astraea.common.DataSize;
import org.astraea.common.FutureUtils;
import org.astraea.common.MapUtils;
import org.astraea.common.admin.Broker;
import org.astraea.common.admin.ConsumerGroup;
import org.astraea.common.admin.NodeInfo;
import org.astraea.common.admin.Partition;
import org.astraea.common.admin.Topic;
import org.astraea.common.admin.TopicConfigs;
import org.astraea.common.admin.TopicPartition;
import org.astraea.common.metrics.broker.HasRate;
import org.astraea.common.metrics.broker.ServerMetrics;
import org.astraea.gui.Context;
import org.astraea.gui.pane.BorderPane;
import org.astraea.gui.pane.PaneBuilder;
import org.astraea.gui.pane.Tab;
import org.astraea.gui.pane.TabPane;
import org.astraea.gui.text.Label;
import org.astraea.gui.text.TextField;

public class TopicTab {

  private static final String TOPIC_NAME_KEY = "topic";

  private static Tab metricsTab(Context context) {
    return Tab.of(
        "metrics",
        PaneBuilder.of()
            .singleRadioButtons(ServerMetrics.Topic.values())
            .searchField("topic name", "topic-*,*abc*")
            .buttonAction(
                (input, logger) ->
                    CompletableFuture.supplyAsync(
                        () -> {
                          var metric =
                              input.singleSelectedRadio(ServerMetrics.Topic.BYTES_IN_PER_SEC);

                          var nodeMeters =
                              context.clients().entrySet().stream()
                                  .collect(
                                      MapUtils.toSortedMap(
                                          Map.Entry::getKey,
                                          entry ->
                                              metric.fetch(entry.getValue()).stream()
                                                  .filter(m -> input.matchSearch(m.topic()))
                                                  .collect(Collectors.toList())));
                          var topics =
                              nodeMeters.values().stream()
                                  .flatMap(Collection::stream)
                                  .map(ServerMetrics.Topic.Meter::topic)
                                  .collect(Collectors.toSet());

                          return topics.stream()
                              .map(
                                  topic -> {
                                    var map = new LinkedHashMap<String, Object>();
                                    map.put("topic", topic);
                                    nodeMeters.forEach(
                                        (nodeInfo, meters) -> {
                                          var key = String.valueOf(nodeInfo.id());
                                          var value =
                                              meters.stream()
                                                  .filter(m -> m.topic().equals(topic))
                                                  .mapToDouble(HasRate::fiveMinuteRate)
                                                  .sum();
                                          switch (metric) {
                                            case BYTES_IN_PER_SEC:
                                            case BYTES_OUT_PER_SEC:
                                              map.put(key, DataSize.Byte.of((long) value));
                                              break;
                                            default:
                                              map.put(key, value);
                                              break;
                                          }
                                        });
                                    return map;
                                  })
                              .collect(Collectors.toList());
                        }))
            .build());
  }

  private static Tab configTab(Context context) {
    return Tab.of(
        "config",
        PaneBuilder.of()
            .searchField("config key", "*policy*")
            .buttonAction(
                (input, logger) ->
                    context
                        .admin()
                        .topicNames(true)
                        .thenCompose(context.admin()::topics)
                        .thenApply(
                            topics -> topics.stream().map(t -> Map.entry(t.name(), t.config())))
                        .thenApply(
                            items ->
                                items
                                    .map(
                                        e -> {
                                          var map = new LinkedHashMap<String, Object>();
                                          map.put("topic", e.getKey());
                                          e.getValue().raw().entrySet().stream()
                                              .filter(entry -> input.matchSearch(entry.getKey()))
                                              .sorted(Map.Entry.comparingByKey())
                                              .forEach(
                                                  entry ->
                                                      map.put(entry.getKey(), entry.getValue()));
                                          return map;
                                        })
                                    .collect(Collectors.toList())))
            .build());
  }

  private static Tab basicTab(Context context) {
    var includeTimestampOfRecord = "record timestamp";
    var includeInternal = "internal";
    return Tab.of(
        "basic",
        PaneBuilder.of()
            .searchField("topic name", "topic-*,*abc*")
            .multiRadioButtons(List.of(includeInternal, includeTimestampOfRecord))
            .tableViewAction(
                Map.of(),
                "DELETE",
                (items, inputs, logger) -> {
                  var topicsToDelete =
                      items.stream()
                          .flatMap(
                              map ->
                                  Optional.ofNullable(map.get(TOPIC_NAME_KEY))
                                      .map(Object::toString)
                                      .stream())
                          .collect(Collectors.toSet());
                  if (topicsToDelete.isEmpty()) {
                    logger.log("nothing to delete");
                    return CompletableFuture.completedStage(null);
                  }
                  return context
                      .admin()
                      .topics(topicsToDelete)
                      .thenCompose(
                          topics -> {
                            var internal =
                                topics.stream()
                                    .filter(Topic::internal)
                                    .map(Topic::name)
                                    .collect(Collectors.toSet());
                            if (!internal.isEmpty())
                              throw new IllegalArgumentException(
                                  "internal topics: " + internal + " can't be deleted");
                            return context
                                .admin()
                                .deleteTopics(
                                    topics.stream().map(Topic::name).collect(Collectors.toSet()));
                          })
                      .thenAccept(
                          ignored -> logger.log("succeed to delete topics: " + topicsToDelete));
                })
            .buttonAction(
                (input, logger) ->
                    context
                        .admin()
                        .topicNames(
                            input.multiSelectedRadios(List.<String>of()).contains(includeInternal))
                        .thenApply(
                            topics ->
                                topics.stream()
                                    .filter(input::matchSearch)
                                    .collect(Collectors.toSet()))
                        .thenCompose(
                            topics ->
                                FutureUtils.combine(
                                    context.admin().brokers(),
                                    context.admin().partitions(topics),
                                    context
                                        .admin()
                                        .consumerGroupIds()
                                        .thenCompose(ids -> context.admin().consumerGroups(ids)),
                                    input
                                            .multiSelectedRadios(List.<String>of())
                                            .contains(includeTimestampOfRecord)
                                        ? context
                                            .admin()
                                            .topicPartitions(topics)
                                            .thenCompose(
                                                tps ->
                                                    context
                                                        .admin()
                                                        .timestampOfLatestRecords(
                                                            tps, Duration.ofSeconds(1)))
                                        : CompletableFuture.completedFuture(
                                            Map.<TopicPartition, Long>of()),
                                    TopicTab::basicResult)))
            .build());
  }

  public static Tab alterTab(Context context) {
    var numberOfPartitions = "number of partitions";
    Function<List<Topic>, BorderPane> toPane =
        topics ->
            BorderPane.selectableTop(
                topics.stream()
                    .collect(
                        MapUtils.toSortedMap(
                            Topic::name,
                            topic ->
                                PaneBuilder.of()
                                    .buttonName("ALTER")
                                    .input(
                                        Label.of(numberOfPartitions),
                                        TextField.builder()
                                            .onlyNumber()
                                            .defaultValue(
                                                String.valueOf(topic.topicPartitions().size()))
                                            .build())
                                    .input(
                                        TopicConfigs.DYNAMICAL_CONFIGS.stream()
                                            .collect(
                                                MapUtils.toSortedMap(
                                                    Label::of,
                                                    k ->
                                                        TextField.builder()
                                                            .defaultValue(
                                                                topic.config().value(k).orElse(""))
                                                            .build())))
                                    .buttonListener(
                                        (input, logger) -> {
                                          var allConfigs = new HashMap<>(input.nonEmptyTexts());
                                          var partitions =
                                              Integer.parseInt(
                                                  allConfigs.remove(numberOfPartitions));
                                          return context
                                              .admin()
                                              .setConfigs(topic.name(), allConfigs)
                                              .thenCompose(
                                                  ignored ->
                                                      context
                                                          .admin()
                                                          .unsetConfigs(
                                                              topic.name(), input.emptyValueKeys()))
                                              .thenCompose(
                                                  ignored ->
                                                      partitions == topic.topicPartitions().size()
                                                          ? CompletableFuture.completedFuture(null)
                                                          : context
                                                              .admin()
                                                              .addPartitions(
                                                                  topic.name(), partitions))
                                              .thenAccept(
                                                  ignored ->
                                                      logger.log(
                                                          "succeed to alter " + topic.name()));
                                        })
                                    .build())));
    return Tab.dynamic(
        "alter",
        () ->
            context
                .admin()
                .topicNames(false)
                .thenCompose(context.admin()::topics)
                .thenApply(toPane));
  }

  public static Tab createTab(Context context) {
    var topicNameKey = "topic";
    var numberOfPartitionsKey = "number of partitions";
    var numberOfReplicasKey = "number of replicas";
    return Tab.of(
        "create",
        PaneBuilder.of()
            .buttonName("CREATE")
            .input(Label.highlight(topicNameKey), TextField.of())
            .input(Label.of(numberOfPartitionsKey), TextField.builder().onlyNumber().build())
            .input(Label.of(numberOfReplicasKey), TextField.builder().onlyNumber().build())
            .input(
                TopicConfigs.ALL_CONFIGS.stream()
                    .collect(Collectors.toMap(Label::of, ignored -> TextField.of())))
            .buttonListener(
                (input, logger) -> {
                  var allConfigs = new HashMap<>(input.nonEmptyTexts());
                  var name = allConfigs.remove(topicNameKey);
                  return context
                      .admin()
                      .topicNames(true)
                      .thenCompose(
                          names -> {
                            if (names.contains(name))
                              return CompletableFuture.failedFuture(
                                  new IllegalArgumentException(name + " is already existent"));

                            return context
                                .admin()
                                .creator()
                                .topic(name)
                                .numberOfPartitions(
                                    Optional.ofNullable(allConfigs.remove(numberOfPartitionsKey))
                                        .map(Integer::parseInt)
                                        .orElse(1))
                                .numberOfReplicas(
                                    Optional.ofNullable(allConfigs.remove(numberOfReplicasKey))
                                        .map(Short::parseShort)
                                        .orElse((short) 1))
                                .configs(allConfigs)
                                .run()
                                .thenAccept(i -> logger.log("succeed to create topic:" + name));
                          });
                })
            .build());
  }

  private static List<Map<String, Object>> basicResult(
      List<Broker> brokers,
      List<Partition> partitions,
      List<ConsumerGroup> groups,
      Map<TopicPartition, Long> timestampOfLatestRecord) {
    var topicSize =
        brokers.stream()
            .flatMap(
                n -> n.dataFolders().stream().flatMap(d -> d.partitionSizes().entrySet().stream()))
            .collect(Collectors.groupingBy(e -> e.getKey().topic()))
            .entrySet()
            .stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    e -> e.getValue().stream().mapToLong(Map.Entry::getValue).sum()));
    var topicPartitions = partitions.stream().collect(Collectors.groupingBy(Partition::topic));
    var topicGroups =
        groups.stream()
            .flatMap(
                g ->
                    g.assignment().values().stream()
                        .flatMap(tps -> tps.stream().map(tp -> Map.entry(tp.topic(), g.groupId()))))
            .collect(Collectors.groupingBy(Map.Entry::getKey))
            .entrySet()
            .stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    e ->
                        e.getValue().stream()
                            .map(Map.Entry::getValue)
                            .collect(Collectors.toSet())));
    var topicTimestampOfLatestRecords =
        timestampOfLatestRecord.entrySet().stream()
            .collect(Collectors.groupingBy(e -> e.getKey().topic()))
            .entrySet()
            .stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    e -> e.getValue().stream().mapToLong(Map.Entry::getValue).max().orElse(-1L)));
    return topicPartitions.keySet().stream()
        .sorted()
        .map(
            topic -> {
              var result = new LinkedHashMap<String, Object>();
              result.put(TOPIC_NAME_KEY, topic);
              result.put(
                  "number of partitions", topicPartitions.getOrDefault(topic, List.of()).size());
              result.put(
                  "number of replicas",
                  topicPartitions.getOrDefault(topic, List.of()).stream()
                      .mapToInt(p -> p.replicas().size())
                      .sum());
              result.put("size", DataSize.Byte.of(topicSize.getOrDefault(topic, 0L)));
              topicPartitions.getOrDefault(topic, List.of()).stream()
                  .flatMap(p -> p.maxTimestamp().stream())
                  .mapToLong(t -> t)
                  .max()
                  .stream()
                  .mapToObj(
                      t -> LocalDateTime.ofInstant(Instant.ofEpochMilli(t), ZoneId.systemDefault()))
                  .findFirst()
                  .ifPresent(t -> result.put("max timestamp", t));
              Optional.ofNullable(topicTimestampOfLatestRecords.get(topic))
                  .ifPresent(
                      t ->
                          result.put(
                              "timestamp of latest record",
                              LocalDateTime.ofInstant(
                                  Instant.ofEpochMilli(t), ZoneId.systemDefault())));
              result.put(
                  "number of active consumers", topicGroups.getOrDefault(topic, Set.of()).size());
              topicPartitions.getOrDefault(topic, List.of()).stream()
                  .flatMap(p -> p.replicas().stream())
                  .collect(Collectors.groupingBy(NodeInfo::id))
                  .entrySet()
                  .stream()
                  .sorted(Map.Entry.comparingByKey())
                  .forEach(
                      entry -> result.put("broker:" + entry.getKey(), entry.getValue().size()));
              return result;
            })
        .collect(Collectors.toList());
  }

  public static Tab of(Context context) {
    return Tab.of(
        "topic",
        TabPane.of(
            Side.TOP,
            List.of(
                basicTab(context),
                PartitionTab.tab(context),
                ReplicaTab.tab(context),
                configTab(context),
                metricsTab(context),
                createTab(context),
                alterTab(context))));
  }
}
