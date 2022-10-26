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
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javafx.geometry.Side;
import org.astraea.common.DataSize;
import org.astraea.common.FutureUtils;
import org.astraea.common.MapUtils;
import org.astraea.common.admin.Broker;
import org.astraea.common.admin.ConsumerGroup;
import org.astraea.common.admin.NodeInfo;
import org.astraea.common.admin.Partition;
import org.astraea.common.admin.TopicConfigs;
import org.astraea.common.admin.TopicPartition;
import org.astraea.common.argument.DurationField;
import org.astraea.common.metrics.broker.HasRate;
import org.astraea.common.metrics.broker.ServerMetrics;
import org.astraea.gui.Context;
import org.astraea.gui.button.SelectBox;
import org.astraea.gui.pane.PaneBuilder;
import org.astraea.gui.pane.Tab;
import org.astraea.gui.pane.TabPane;
import org.astraea.gui.text.EditableText;
import org.astraea.gui.text.NoneditableText;

public class TopicTab {

  private static final String TOPIC_NAME_KEY = "topic";
  private static final String INTERNAL_KEY = "internal";

  private static Tab metricsTab(Context context) {
    return Tab.of(
        "metrics",
        PaneBuilder.of()
            .selectBox(
                SelectBox.single(
                    Arrays.stream(ServerMetrics.Topic.values())
                        .map(ServerMetrics.Topic::toString)
                        .collect(Collectors.toList()),
                    ServerMetrics.Topic.values().length))
            .buttonAction(
                (input, logger) ->
                    CompletableFuture.supplyAsync(
                        () -> {
                          var metric =
                              input.selectedKeys().stream()
                                  .flatMap(
                                      name ->
                                          Arrays.stream(ServerMetrics.Topic.values())
                                              .filter(c -> c.alias().equals(name)))
                                  .findFirst()
                                  .orElse(ServerMetrics.Topic.BYTES_IN_PER_SEC);

                          var nodeMeters =
                              context.clients().entrySet().stream()
                                  .collect(
                                      MapUtils.toSortedMap(
                                          Map.Entry::getKey,
                                          entry -> metric.fetch(entry.getValue())));
                          var topics =
                              nodeMeters.values().stream()
                                  .flatMap(Collection::stream)
                                  .map(ServerMetrics.Topic.Meter::topic)
                                  .collect(Collectors.toSet());

                          return topics.stream()
                              .map(
                                  topic -> {
                                    var map = new LinkedHashMap<String, Object>();
                                    map.put(TOPIC_NAME_KEY, topic);
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
            .buttonAction(
                (input, logger) ->
                    FutureUtils.combine(
                        context
                            .admin()
                            .topicNames(true)
                            .thenCompose(
                                names ->
                                    context
                                        .admin()
                                        .topics(names)
                                        .thenApply(
                                            topics ->
                                                topics.stream()
                                                    .map(
                                                        t -> Map.entry(t.name(), t.config().raw())))
                                        .exceptionally(
                                            e -> {
                                              // previous kafka does not check topic config. The
                                              // configs
                                              // APIs return error if there are corrupted configs
                                              logger.log(e.getMessage());
                                              return names.stream()
                                                  .map(n -> Map.entry(n, Map.of()));
                                            })),
                        context.admin().internalTopicNames(),
                        (topics, internalNames) ->
                            topics
                                .map(
                                    e -> {
                                      var map = new LinkedHashMap<String, Object>();
                                      map.put(TOPIC_NAME_KEY, e.getKey());
                                      map.put(INTERNAL_KEY, internalNames.contains(e.getKey()));
                                      map.putAll(new TreeMap<>(e.getValue()));
                                      return map;
                                    })
                                .collect(Collectors.toList())))
            .tableViewAction(
                MapUtils.of(
                    NoneditableText.of(TopicConfigs.ALL_CONFIGS),
                    EditableText.singleLine().build()),
                "ALERT",
                (tables, input, logger) -> {
                  var topicsToAlter =
                      tables.stream()
                          .flatMap(
                              m ->
                                  Optional.ofNullable(m.get(TOPIC_NAME_KEY))
                                      .map(Object::toString)
                                      .stream())
                          .collect(Collectors.toSet());
                  if (topicsToAlter.isEmpty()) {
                    logger.log("nothing to alter");
                    return CompletableFuture.completedStage(null);
                  }
                  return context
                      .admin()
                      .internalTopicNames()
                      .thenCompose(
                          internalTopics -> {
                            var internal =
                                topicsToAlter.stream()
                                    .filter(internalTopics::contains)
                                    .collect(Collectors.toSet());
                            if (!internal.isEmpty())
                              throw new IllegalArgumentException(
                                  "internal topics: " + internal + " can't be altered");
                            var unsets = input.emptyValueKeys();
                            var sets = input.nonEmptyTexts();
                            if (unsets.isEmpty() && sets.isEmpty()) {
                              logger.log("nothing to alter");
                              return CompletableFuture.completedStage(null);
                            }
                            return FutureUtils.sequence(
                                    topicsToAlter.stream()
                                        .flatMap(
                                            topic ->
                                                Stream.of(
                                                    context
                                                        .admin()
                                                        .setConfigs(topic, sets)
                                                        .toCompletableFuture(),
                                                    context
                                                        .admin()
                                                        .unsetConfigs(topic, unsets)
                                                        .toCompletableFuture()))
                                        .collect(Collectors.toList()))
                                .thenAccept(
                                    ignored -> logger.log("succeed to alter: " + topicsToAlter));
                          });
                })
            .build());
  }

  private static Tab basicTab(Context context) {
    var includeTimestampOfRecord = "max time to wait records";
    return Tab.of(
        "basic",
        PaneBuilder.of()
            .input(
                NoneditableText.of(includeTimestampOfRecord),
                EditableText.singleLine().hint("3s").build())
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
                      .internalTopicNames()
                      .thenCompose(
                          internalTopics -> {
                            var internal =
                                topicsToDelete.stream()
                                    .filter(internalTopics::contains)
                                    .collect(Collectors.toSet());
                            if (!internal.isEmpty())
                              throw new IllegalArgumentException(
                                  "internal topics: " + internal + " can't be deleted");
                            return context.admin().deleteTopics(topicsToDelete);
                          })
                      .thenAccept(
                          ignored -> logger.log("succeed to delete topics: " + topicsToDelete));
                })
            .buttonAction(
                (input, logger) ->
                    context
                        .admin()
                        .topicNames(true)
                        .thenCompose(
                            topics ->
                                FutureUtils.combine(
                                    context.admin().brokers(),
                                    context.admin().partitions(topics),
                                    context
                                        .admin()
                                        .consumerGroupIds()
                                        .thenCompose(ids -> context.admin().consumerGroups(ids)),
                                    context
                                        .admin()
                                        .topicPartitions(topics)
                                        .thenCompose(
                                            tps ->
                                                input
                                                    .get(includeTimestampOfRecord)
                                                    .map(DurationField::toDuration)
                                                    .map(
                                                        v ->
                                                            context
                                                                .admin()
                                                                .timestampOfLatestRecords(tps, v))
                                                    .orElseGet(
                                                        () ->
                                                            CompletableFuture.completedFuture(
                                                                Map.<TopicPartition, Long>of()))),
                                    TopicTab::basicResult)))
            .build());
  }

  public static Tab createTab(Context context) {
    var numberOfPartitionsKey = "number of partitions";
    var numberOfReplicasKey = "number of replicas";
    return Tab.of(
        "create",
        PaneBuilder.of()
            .buttonName("CREATE")
            .input(
                NoneditableText.highlight(TOPIC_NAME_KEY),
                EditableText.singleLine().disallowEmpty().build())
            .input(
                NoneditableText.of(numberOfPartitionsKey),
                EditableText.singleLine().onlyNumber().build())
            .input(
                NoneditableText.of(numberOfReplicasKey),
                EditableText.singleLine().onlyNumber().build())
            .input(
                TopicConfigs.ALL_CONFIGS.stream()
                    .collect(
                        Collectors.toMap(
                            NoneditableText::of, ignored -> EditableText.singleLine().build())))
            .buttonListener(
                (input, logger) -> {
                  var allConfigs = new HashMap<>(input.nonEmptyTexts());
                  var name = allConfigs.remove(TOPIC_NAME_KEY);
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
              var ps = topicPartitions.getOrDefault(topic, List.of());
              var result = new LinkedHashMap<String, Object>();
              result.put(TOPIC_NAME_KEY, topic);
              ps.stream().findFirst().ifPresent(p -> result.put(INTERNAL_KEY, p.internal()));
              result.put("number of partitions", ps.size());
              result.put(
                  "number of replicas", ps.stream().mapToInt(p -> p.replicas().size()).sum());
              result.put("size", DataSize.Byte.of(topicSize.getOrDefault(topic, 0L)));
              ps.stream().flatMap(p -> p.maxTimestamp().stream()).mapToLong(t -> t).max().stream()
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
              ps.stream()
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
        TOPIC_NAME_KEY,
        TabPane.of(
            Side.TOP,
            List.of(
                basicTab(context),
                PartitionTab.tab(context),
                ReplicaTab.tab(context),
                configTab(context),
                metricsTab(context),
                createTab(context))));
  }
}
