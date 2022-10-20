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
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import javafx.geometry.Side;
import org.astraea.common.DataSize;
import org.astraea.common.FutureUtils;
import org.astraea.common.MapUtils;
import org.astraea.common.Utils;
import org.astraea.common.admin.Broker;
import org.astraea.common.admin.ConsumerGroup;
import org.astraea.common.admin.NodeInfo;
import org.astraea.common.admin.Partition;
import org.astraea.common.admin.Topic;
import org.astraea.common.admin.TopicChecker;
import org.astraea.common.admin.TopicConfigs;
import org.astraea.common.admin.TopicPartition;
import org.astraea.common.argument.DurationField;
import org.astraea.common.metrics.broker.HasRate;
import org.astraea.common.metrics.broker.ServerMetrics;
import org.astraea.gui.Context;
import org.astraea.gui.pane.BorderPane;
import org.astraea.gui.pane.PaneBuilder;
import org.astraea.gui.pane.Tab;
import org.astraea.gui.pane.TabPane;

public class TopicTab {

  private static Tab metricsTab(Context context) {
    return Tab.of(
        "metrics",
        PaneBuilder.of()
            .radioButtons(ServerMetrics.Topic.values())
            .searchField("topic name")
            .buttonAction(
                (input, logger) ->
                    CompletableFuture.supplyAsync(
                        () -> {
                          var metric =
                              input
                                  .selectedRadio()
                                  .map(o -> (ServerMetrics.Topic) o)
                                  .orElse(ServerMetrics.Topic.BYTES_IN_PER_SEC);
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
            .searchField("config key")
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

  private static Tab idleTab(Context context) {
    var expiredDays = "expired (<number><unit>)";
    return Tab.of(
        "no write",
        PaneBuilder.of()
            .input(expiredDays, false, false)
            .searchField("topic name")
            .buttonAction(
                (input, logger) ->
                    context
                        .admin()
                        .idleTopic(
                            List.of(
                                TopicChecker.latestTimestamp(
                                    DurationField.toDuration(
                                        input.nonEmptyTexts().getOrDefault(expiredDays, "30day")),
                                    Duration.ofSeconds(1))))
                        .thenApply(
                            topics ->
                                topics.stream()
                                    .filter(input::matchSearch)
                                    .collect(Collectors.toSet()))
                        .thenCompose(
                            topics ->
                                tuple(context, topics, true)
                                    .thenApply(tuple -> basicResult(tuple, ignored -> true))))
            .build());
  }

  private static Tab basicTab(Context context) {
    return Tab.of(
        "basic",
        PaneBuilder.of()
            .searchField("topic name")
            .buttonAction(
                (input, logger) ->
                    context
                        .admin()
                        .topicNames(true)
                        .thenApply(
                            topics ->
                                topics.stream()
                                    .filter(input::matchSearch)
                                    .collect(Collectors.toSet()))
                        .thenCompose(
                            topics ->
                                tuple(context, topics, false)
                                    .thenApply(tuple -> basicResult(tuple, ignored -> true))))
            .build());
  }

  public static Tab alterTab(Context context) {
    var numberOfPartitions = "number of partitions";
    return Tab.dynamic(
        "alter",
        () ->
            context
                .admin()
                .topicNames(false)
                .thenCompose(context.admin()::topics)
                .thenApply(
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
                                                    numberOfPartitions,
                                                    false,
                                                    true,
                                                    false,
                                                    String.valueOf(topic.topicPartitions().size()))
                                                .input(
                                                    TopicConfigs.DYNAMICAL_CONFIGS.stream()
                                                        .collect(
                                                            Collectors.toMap(
                                                                k -> k,
                                                                k ->
                                                                    topic
                                                                        .config()
                                                                        .value(k)
                                                                        .orElse(""))))
                                                .buttonListener(
                                                    (input, logger) -> {
                                                      var allConfigs =
                                                          new HashMap<>(input.nonEmptyTexts());
                                                      var partitions =
                                                          Integer.parseInt(
                                                              allConfigs.remove(
                                                                  numberOfPartitions));
                                                      return context
                                                          .admin()
                                                          .setConfigs(topic.name(), allConfigs)
                                                          .thenCompose(
                                                              ignored ->
                                                                  context
                                                                      .admin()
                                                                      .unsetConfigs(
                                                                          topic.name(),
                                                                          input.emptyValueKeys()))
                                                          .thenCompose(
                                                              ignored ->
                                                                  partitions
                                                                          == topic
                                                                              .topicPartitions()
                                                                              .size()
                                                                      ? CompletableFuture
                                                                          .completedFuture(null)
                                                                      : context
                                                                          .admin()
                                                                          .addPartitions(
                                                                              topic.name(),
                                                                              partitions))
                                                          .thenAccept(
                                                              ignored ->
                                                                  logger.log(
                                                                      "succeed to alter "
                                                                          + topic.name()));
                                                    })
                                                .build())))));
  }

  public static Tab createTab(Context context) {
    var topicNameKey = "topic";
    var numberOfPartitionsKey = "number of partitions";
    var numberOfReplicasKey = "number of replicas";
    return Tab.of(
        "create",
        PaneBuilder.of()
            .buttonName("CREATE")
            .input(topicNameKey, true, false)
            .input(numberOfPartitionsKey, false, true)
            .input(numberOfReplicasKey, false, true)
            .input(TopicConfigs.ALL_CONFIGS)
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

  private static Tab deleteTab(Context context) {
    return Tab.dynamic(
        "delete",
        () -> {
          var topicsFuture = context.admin().topicNames(false);
          return FutureUtils.combine(
              topicsFuture,
              topicsFuture.thenCompose(topics -> tuple(context, topics, false)),
              (topics, tuple) ->
                  BorderPane.selectableTop(
                      topics.stream()
                          .collect(
                              MapUtils.toSortedMap(
                                  Function.identity(),
                                  topic ->
                                      PaneBuilder.of()
                                          .buttonName("DELETE")
                                          .initTableView(basicResult(tuple, topic::equals))
                                          .buttonListener(
                                              (input, logger) ->
                                                  context
                                                      .admin()
                                                      .deleteTopics(Set.of(topic))
                                                      .thenAccept(
                                                          ignored ->
                                                              logger.log(
                                                                  "succeed to delete " + topic)))
                                          .build()))));
        });
  }

  public static Tab of(Context context) {
    return Tab.of(
        "topic",
        TabPane.of(
            Side.TOP,
            List.of(
                basicTab(context),
                configTab(context),
                metricsTab(context),
                createTab(context),
                alterTab(context),
                idleTab(context),
                deleteTab(context))));
  }

  private static List<Map<String, Object>> basicResult(Tuple tuple, Predicate<String> topicFilter) {
    var topicSize =
        tuple.brokers.stream()
            .flatMap(
                n -> n.dataFolders().stream().flatMap(d -> d.partitionSizes().entrySet().stream()))
            .collect(Collectors.groupingBy(e -> e.getKey().topic()))
            .entrySet()
            .stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    e -> e.getValue().stream().mapToLong(Map.Entry::getValue).sum()));
    var topicPartitions =
        tuple.partitions.stream().collect(Collectors.groupingBy(Partition::topic));
    var topicGroups =
        tuple.groups.stream()
            .flatMap(
                g ->
                    g.consumeProgress().keySet().stream()
                        .map(tp -> Map.entry(tp.topic(), g.groupId())))
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
        tuple.timestampOfLatestRecord.entrySet().stream()
            .collect(Collectors.groupingBy(e -> e.getKey().topic()))
            .entrySet()
            .stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    e -> e.getValue().stream().mapToLong(Map.Entry::getValue).max().orElse(-1L)));
    return topicPartitions.keySet().stream()
        .filter(topicFilter)
        .sorted()
        .map(
            topic -> {
              var result = new LinkedHashMap<String, Object>();
              result.put("topic", topic);
              result.put(
                  "number of partitions", topicPartitions.getOrDefault(topic, List.of()).size());
              result.put(
                  "number of replicas",
                  topicPartitions.getOrDefault(topic, List.of()).stream()
                      .mapToInt(p -> p.replicas().size())
                      .sum());
              result.put("size", DataSize.Byte.of(topicSize.getOrDefault(topic, 0L)));
              topicPartitions.getOrDefault(topic, List.of()).stream()
                  .mapToLong(Partition::maxTimestamp)
                  .max()
                  .stream()
                  .mapToObj(Utils::format)
                  .findFirst()
                  .ifPresent(t -> result.put("max timestamp", t));
              Optional.ofNullable(topicTimestampOfLatestRecords.get(topic))
                  .ifPresent(t -> result.put("timestamp of latest record", Utils.format(t)));
              result.put(
                  "number of consumer groups", topicGroups.getOrDefault(topic, Set.of()).size());
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

  private static CompletionStage<Tuple> tuple(
      Context context, Set<String> topics, boolean needPollRecords) {
    return FutureUtils.combine(
        context.admin().brokers(),
        context.admin().partitions(topics),
        context.admin().consumerGroupIds().thenCompose(ids -> context.admin().consumerGroups(ids)),
        needPollRecords
            ? context
                .admin()
                .topicPartitions(topics)
                .thenCompose(
                    tps -> context.admin().timestampOfLatestRecords(tps, Duration.ofSeconds(1)))
            : CompletableFuture.completedFuture(Map.of()),
        Tuple::new);
  }

  private static class Tuple {
    private final List<Partition> partitions;
    private final List<Broker> brokers;
    private final List<ConsumerGroup> groups;

    private final Map<TopicPartition, Long> timestampOfLatestRecord;

    private Tuple(
        List<Broker> brokers,
        List<Partition> partitions,
        List<ConsumerGroup> groups,
        Map<TopicPartition, Long> timestampOfLatestRecord) {
      this.partitions = partitions;
      this.brokers = brokers;
      this.groups = groups;
      this.timestampOfLatestRecord = timestampOfLatestRecord;
    }
  }
}
