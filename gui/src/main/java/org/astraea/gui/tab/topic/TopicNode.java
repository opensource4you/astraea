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
import java.util.function.Function;
import java.util.stream.Collectors;
import javafx.geometry.Side;
import javafx.scene.Node;
import org.astraea.common.DataSize;
import org.astraea.common.FutureUtils;
import org.astraea.common.MapUtils;
import org.astraea.common.admin.Broker;
import org.astraea.common.admin.ConsumerGroup;
import org.astraea.common.admin.NodeInfo;
import org.astraea.common.admin.Partition;
import org.astraea.common.admin.ProducerState;
import org.astraea.common.admin.Topic;
import org.astraea.common.admin.TopicConfigs;
import org.astraea.common.metrics.broker.HasRate;
import org.astraea.common.metrics.broker.ServerMetrics;
import org.astraea.gui.Context;
import org.astraea.gui.button.SelectBox;
import org.astraea.gui.pane.PaneBuilder;
import org.astraea.gui.pane.Slide;
import org.astraea.gui.text.EditableText;
import org.astraea.gui.text.TextInput;

public class TopicNode {

  private static final String TOPIC_NAME_KEY = "topic";
  private static final String INTERNAL_KEY = "internal";

  private static Node metricsNode(Context context) {
    var selectBox =
        SelectBox.single(
            Arrays.stream(ServerMetrics.Topic.values())
                .map(ServerMetrics.Topic::toString)
                .collect(Collectors.toList()),
            ServerMetrics.Topic.values().length);
    return PaneBuilder.of()
        .firstPart(
            selectBox,
            "REFRESH",
            (argument, logger) ->
                CompletableFuture.supplyAsync(
                    () -> {
                      var metric =
                          argument.selectedKeys().stream()
                              .flatMap(
                                  name ->
                                      Arrays.stream(ServerMetrics.Topic.values())
                                          .filter(c -> c.alias().equals(name)))
                              .findFirst()
                              .orElse(ServerMetrics.Topic.BYTES_IN_PER_SEC);

                      var nodeMeters =
                          context.brokerClients().entrySet().stream()
                              .collect(
                                  MapUtils.toSortedMap(
                                      Map.Entry::getKey, entry -> metric.fetch(entry.getValue())));
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
                                    (nodeId, meters) -> {
                                      var key = String.valueOf(nodeId);
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
        .build();
  }

  private static Node configNode(Context context) {
    return PaneBuilder.of()
        .firstPart(
            "REFRESH",
            (argument, logger) ->
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
                                                .map(t -> Map.entry(t.name(), t.config().raw())))
                                    .exceptionally(
                                        e -> {
                                          // previous kafka does not check topic config. The
                                          // configs
                                          // APIs return error if there are corrupted configs
                                          logger.log(e.getMessage());
                                          return names.stream().map(n -> Map.entry(n, Map.of()));
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
        .secondPart(
            List.of(
                TextInput.of(
                    TopicConfigs.SEGMENT_BYTES_CONFIG,
                    TopicConfigs.ALL_CONFIGS,
                    EditableText.singleLine().disable().build())),
            "ALTER",
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
                        var unset =
                            topicsToAlter.stream()
                                .collect(
                                    Collectors.toMap(
                                        Function.identity(), b -> input.emptyValueKeys()));
                        var set =
                            topicsToAlter.stream()
                                .collect(
                                    Collectors.toMap(
                                        Function.identity(), b -> input.nonEmptyTexts()));
                        if (unset.isEmpty() && set.isEmpty()) {
                          logger.log("nothing to alter");
                          return CompletableFuture.completedStage(null);
                        }
                        return context
                            .admin()
                            .unsetTopicConfigs(unset)
                            .thenCompose(ignored -> context.admin().setTopicConfigs(set))
                            .thenAccept(
                                ignored -> logger.log("succeed to alter: " + topicsToAlter));
                      });
            })
        .build();
  }

  private static Node basicNode(Context context) {
    return PaneBuilder.of()
        .secondPart(
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
                  .thenAccept(ignored -> logger.log("succeed to delete topics: " + topicsToDelete));
            })
        .firstPart(
            "REFRESH",
            (argument, logger) ->
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
                                    .thenCompose(tps -> context.admin().producerStates(tps)),
                                TopicNode::basicResult)))
        .build();
  }

  public static Node createNode(Context context) {
    var numberOfPartitionsKey = "number of partitions";
    var numberOfReplicasKey = "number of replicas";

    var multiInput =
        List.of(
            TextInput.required(TOPIC_NAME_KEY, EditableText.singleLine().disallowEmpty().build()),
            TextInput.of(numberOfPartitionsKey, EditableText.singleLine().onlyNumber().build()),
            TextInput.of(numberOfReplicasKey, EditableText.singleLine().onlyNumber().build()),
            TextInput.of(
                TopicConfigs.CLEANUP_POLICY_CONFIG,
                TopicConfigs.ALL_CONFIGS,
                EditableText.singleLine().build()),
            TextInput.of(
                TopicConfigs.COMPRESSION_TYPE_CONFIG,
                TopicConfigs.ALL_CONFIGS,
                EditableText.singleLine().build()),
            TextInput.of(
                TopicConfigs.MESSAGE_TIMESTAMP_TYPE_CONFIG,
                TopicConfigs.ALL_CONFIGS,
                EditableText.singleLine().build()));

    return PaneBuilder.of()
        .firstPart(
            multiInput,
            "CREATE",
            (argument, logger) -> {
              var allConfigs = new HashMap<>(argument.nonEmptyTexts());
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
                            .thenApply(
                                i -> {
                                  logger.log("succeed to create topic:" + name);
                                  return List.of();
                                });
                      });
            })
        .build();
  }

  private static List<Map<String, Object>> basicResult(
      List<Broker> brokers,
      List<Partition> partitions,
      List<ConsumerGroup> groups,
      List<ProducerState> producerStates) {
    var topicSize =
        brokers.stream()
            .flatMap(
                n -> n.dataFolders().stream().flatMap(d -> d.partitionSizes().entrySet().stream()))
            .collect(
                Collectors.groupingBy(
                    e -> e.getKey().topic(),
                    Collectors.mapping(Map.Entry::getValue, Collectors.reducing(0L, Long::sum))));

    var topicPartitions = partitions.stream().collect(Collectors.groupingBy(Partition::topic));
    var topicGroups =
        groups.stream()
            .flatMap(
                g ->
                    g.assignment().values().stream()
                        .flatMap(tps -> tps.stream().map(tp -> Map.entry(tp.topic(), g.groupId()))))
            .collect(
                Collectors.groupingBy(
                    Map.Entry::getKey,
                    Collectors.mapping(Map.Entry::getValue, Collectors.toSet())));
    var topicProducers =
        producerStates.stream()
            .collect(
                Collectors.groupingBy(
                    ProducerState::topic,
                    Collectors.mapping(ProducerState::producerId, Collectors.toSet())));
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
              result.put(
                  "number of consumer group", topicGroups.getOrDefault(topic, Set.of()).size());
              // producer states is supported by kafka 2.8.0+
              if (!topicProducers.isEmpty())
                result.put(
                    "number of producer id", topicProducers.getOrDefault(topic, Set.of()).size());
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

  public static Node healthNode(Context context) {
    return PaneBuilder.of()
        .firstPart(
            null,
            List.of(),
            "CHECK",
            (argument, logger) ->
                context
                    .admin()
                    .topicNames(true)
                    .thenCompose(
                        names ->
                            FutureUtils.combine(
                                context.admin().topics(names),
                                context.admin().partitions(names),
                                (topics, partitions) -> {
                                  var minInSync =
                                      topics.stream()
                                          .collect(
                                              Collectors.toMap(
                                                  Topic::name,
                                                  t ->
                                                      t.config()
                                                          .value(
                                                              TopicConfigs
                                                                  .MIN_IN_SYNC_REPLICAS_CONFIG)
                                                          .map(Integer::parseInt)
                                                          .orElse(1)));
                                  var result =
                                      new LinkedHashMap<String, List<Map<String, Object>>>();

                                  var unavailablePartitions =
                                      partitions.stream()
                                          .filter(
                                              p ->
                                                  p.isr().size()
                                                          < minInSync.getOrDefault(p.topic(), 1)
                                                      || p.leader().isEmpty())
                                          .map(
                                              p -> {
                                                var r = new LinkedHashMap<String, Object>();
                                                r.put("topic", p.topic());
                                                r.put("partition", p.partition());
                                                r.put(
                                                    "leader",
                                                    p.leader()
                                                        .map(n -> String.valueOf(n.id()))
                                                        .orElse("null"));
                                                r.put(
                                                    "in-sync replicas",
                                                    p.isr().stream()
                                                        .map(n -> String.valueOf(n.id()))
                                                        .collect(Collectors.joining(",")));
                                                r.put(
                                                    TopicConfigs.MIN_IN_SYNC_REPLICAS_CONFIG,
                                                    minInSync.getOrDefault(p.topic(), 1));
                                                r.put("readable", p.leader().isPresent());
                                                r.put(
                                                    "writable",
                                                    p.leader().isPresent()
                                                        && p.isr().size()
                                                            >= minInSync.getOrDefault(
                                                                p.topic(), 1));
                                                return (Map<String, Object>) r;
                                              })
                                          .collect(Collectors.toList());
                                  result.put("unavailable partitions", unavailablePartitions);
                                  return result;
                                })))
        .build();
  }

  public static Node of(Context context) {
    return Slide.of(
            Side.TOP,
            MapUtils.of(
                "basic", basicNode(context),
                "partition", PartitionNode.of(context),
                "replica", ReplicaNode.of(context),
                "config", configNode(context),
                "metrics", metricsNode(context),
                "health", healthNode(context),
                "create", createNode(context)))
        .node();
  }
}
