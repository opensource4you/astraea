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

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javafx.geometry.Side;
import javafx.scene.Node;
import org.astraea.common.DataSize;
import org.astraea.common.FutureUtils;
import org.astraea.common.MapUtils;
import org.astraea.common.admin.Broker;
import org.astraea.common.admin.BrokerConfigs;
import org.astraea.common.admin.TopicPartition;
import org.astraea.common.metrics.MBeanClient;
import org.astraea.common.metrics.broker.ControllerMetrics;
import org.astraea.common.metrics.broker.LogMetrics;
import org.astraea.common.metrics.broker.ServerMetrics;
import org.astraea.common.metrics.platform.HostMetrics;
import org.astraea.gui.Context;
import org.astraea.gui.button.SelectBox;
import org.astraea.gui.pane.PaneBuilder;
import org.astraea.gui.pane.Tab;
import org.astraea.gui.pane.TabPane;
import org.astraea.gui.text.EditableText;
import org.astraea.gui.text.NoneditableText;

public class BrokerTab {

  private static final String BROKER_ID_KEY = "broker id";

  private static <T> Optional<T> tryToFetch(Supplier<T> function) {
    try {
      return Optional.of(function.get());
    } catch (Exception e) {
      return Optional.empty();
    }
  }

  private enum MetricType {
    INFO(
        "info",
        client ->
            ServerMetrics.appInfo(client).stream()
                .findFirst()
                .map(
                    appInfo -> {
                      Map<String, Object> result = new LinkedHashMap<>();
                      result.put("version", appInfo.version());
                      result.put("revision", appInfo.commitId());
                      appInfo
                          .startTimeMs()
                          .ifPresent(
                              t ->
                                  result.put(
                                      "start time",
                                      LocalDateTime.ofInstant(
                                          Instant.ofEpochMilli(t), ZoneId.systemDefault())));
                      return result;
                    })
                .orElse(Map.of())),
    ZOOKEEPER_REQUEST(
        "zookeeper request",
        client ->
            Arrays.stream(ServerMetrics.ZooKeeperClientMetrics.values())
                .flatMap(m -> tryToFetch(() -> m.fetch(client)).stream())
                .collect(Collectors.toMap(m -> m.metricsName(), m -> m.percentile50()))),
    ZOOKEEPER_SESSION(
        "zookeeper session",
        client ->
            Arrays.stream(ServerMetrics.SessionExpireListener.values())
                .flatMap(m -> tryToFetch(() -> m.fetch(client)).stream())
                .collect(Collectors.toMap(m -> m.metricsName(), m -> m.fiveMinuteRate()))),
    HOST(
        "host",
        client ->
            tryToFetch(() -> HostMetrics.operatingSystem(client))
                .map(o -> o.beanObject().attributes())
                .orElse(Map.of())),
    CONTROLLER(
        "controller",
        client ->
            Arrays.stream(ControllerMetrics.Controller.values())
                .flatMap(m -> tryToFetch(() -> m.fetch(client)).stream())
                .collect(Collectors.toMap(m -> m.metricsName(), m -> m.value()))),

    CONTROLLER_STATE(
        "controller state",
        client ->
            Arrays.stream(ControllerMetrics.ControllerState.values())
                .flatMap(m -> tryToFetch(() -> m.fetch(client)).stream())
                .collect(Collectors.toMap(m -> m.metricsName(), m -> m.fiveMinuteRate()))),
    NETWORK(
        "network",
        client ->
            tryToFetch(() -> ServerMetrics.Socket.socketNetworkProcessor(client))
                .map(
                    result ->
                        result.stream()
                            .flatMap(
                                r ->
                                    r.beanObject().attributes().entrySet().stream()
                                        .map(
                                            o ->
                                                Map.entry(
                                                    r.listener()
                                                        + ":"
                                                        + r.networkProcessor()
                                                        + ":"
                                                        + o.getKey(),
                                                    o.getKey().toLowerCase().contains("byte")
                                                        ? DataSize.Byte.of(
                                                            (long) (double) o.getValue())
                                                        : o.getValue())))
                            .collect(
                                org.astraea.common.MapUtils.toSortedMap(
                                    Map.Entry::getKey, Map.Entry::getValue)))
                .orElse(new TreeMap<>())),

    DELAYED_OPERATION(
        "delayed operation",
        client ->
            Arrays.stream(ServerMetrics.DelayedOperationPurgatory.values())
                .flatMap(m -> tryToFetch(() -> m.fetch(client)).stream())
                .collect(Collectors.toMap(m -> m.metricsName(), m -> m.value()))),

    REPLICA(
        "replica",
        client ->
            Arrays.stream(ServerMetrics.ReplicaManager.values())
                .flatMap(m -> tryToFetch(() -> m.fetch(client)).stream())
                .collect(Collectors.toMap(m -> m.metricsName(), m -> m.value()))),
    BROKER_TOPIC(
        "broker topic",
        client ->
            Arrays.stream(ServerMetrics.BrokerTopic.values())
                .flatMap(m -> tryToFetch(() -> m.fetch(client)).stream())
                .collect(
                    Collectors.toMap(
                        m -> m.metricsName(),
                        m -> {
                          switch (m.type()) {
                            case BYTES_IN_PER_SEC:
                            case BYTES_OUT_PER_SEC:
                            case BYTES_REJECTED_PER_SEC:
                            case REASSIGNMENT_BYTES_OUT_PER_SEC:
                            case REASSIGNMENT_BYTES_IN_PER_SEC:
                            case REPLICATION_BYTES_IN_PER_SEC:
                            case REPLICATION_BYTES_OUT_PER_SEC:
                              return DataSize.Byte.of((long) m.fiveMinuteRate());
                            default:
                              return m.fiveMinuteRate();
                          }
                        })));

    private final Function<MBeanClient, Map<String, Object>> fetcher;
    private final String display;

    MetricType(String display, Function<MBeanClient, Map<String, Object>> fetcher) {
      this.display = display;
      this.fetcher = fetcher;
    }

    @Override
    public String toString() {
      return display;
    }
  }

  static Tab metricsTab(Context context) {
    return Tab.of(
        "metrics",
        PaneBuilder.of()
            .selectBox(
                SelectBox.multi(
                    Arrays.stream(MetricType.values())
                        .map(Enum::toString)
                        .collect(Collectors.toList()),
                    MetricType.values().length / 2))
            .buttonAction(
                (input, logger) ->
                    context
                        .admin()
                        .nodeInfos()
                        .thenApply(
                            nodes ->
                                context.clients(nodes).entrySet().stream()
                                    .flatMap(
                                        entry ->
                                            input.selectedKeys().stream()
                                                .flatMap(
                                                    name ->
                                                        Arrays.stream(MetricType.values())
                                                            .filter(m -> m.toString().equals(name)))
                                                .map(
                                                    m ->
                                                        Map.entry(
                                                            entry.getKey(),
                                                            m.fetcher.apply(entry.getValue()))))
                                    .collect(Collectors.groupingBy(Map.Entry::getKey))
                                    .entrySet()
                                    .stream()
                                    .map(
                                        entry -> {
                                          var result = new LinkedHashMap<String, Object>();
                                          result.put(BROKER_ID_KEY, entry.getKey().id());
                                          entry.getValue().stream()
                                              .flatMap(e -> e.getValue().entrySet().stream())
                                              .sorted(
                                                  Comparator.comparing(
                                                      e -> e.getKey().toLowerCase()))
                                              .forEach(e -> result.put(e.getKey(), e.getValue()));
                                          return result;
                                        })
                                    .collect(Collectors.toList())))
            .build());
  }

  private static List<Map<String, Object>> basicResult(List<Broker> brokers) {
    return brokers.stream()
        .map(
            broker ->
                MapUtils.<String, Object>of(
                    BROKER_ID_KEY,
                    broker.id(),
                    "hostname",
                    broker.host(),
                    "port",
                    broker.port(),
                    "controller",
                    broker.isController(),
                    "topics",
                    broker.dataFolders().stream()
                        .flatMap(
                            d -> d.partitionSizes().keySet().stream().map(TopicPartition::topic))
                        .distinct()
                        .count(),
                    "partitions",
                    broker.dataFolders().stream()
                        .flatMap(d -> d.partitionSizes().keySet().stream())
                        .distinct()
                        .count(),
                    "leaders",
                    broker.topicPartitionLeaders().size(),
                    "size",
                    DataSize.Byte.of(
                        broker.dataFolders().stream()
                            .mapToLong(
                                d -> d.partitionSizes().values().stream().mapToLong(v -> v).sum())
                            .sum()),
                    "orphan partitions",
                    broker.dataFolders().stream()
                        .flatMap(d -> d.orphanPartitionSizes().keySet().stream())
                        .distinct()
                        .count(),
                    "orphan size",
                    DataSize.Byte.of(
                        broker.dataFolders().stream()
                            .mapToLong(
                                d ->
                                    d.orphanPartitionSizes().values().stream()
                                        .mapToLong(v -> v)
                                        .sum())
                            .sum())))
        .collect(Collectors.toList());
  }

  private static Tab basicTab(Context context) {
    return Tab.of(
        "basic",
        PaneBuilder.of()
            .buttonAction(
                (input, logger) -> context.admin().brokers().thenApply(BrokerTab::basicResult))
            .build());
  }

  private static Tab configTab(Context context) {
    return Tab.of(
        "config",
        PaneBuilder.of()
            .buttonAction(
                (input, logger) ->
                    context
                        .admin()
                        .brokers()
                        .thenApply(
                            brokers -> brokers.stream().map(t -> Map.entry(t.id(), t.config())))
                        .thenApply(
                            items ->
                                items
                                    .map(
                                        e -> {
                                          var map = new LinkedHashMap<String, Object>();
                                          map.put(BROKER_ID_KEY, e.getKey());
                                          map.putAll(new TreeMap<>(e.getValue().raw()));
                                          return map;
                                        })
                                    .collect(Collectors.toList())))
            .tableViewAction(
                MapUtils.of(
                    NoneditableText.of(BrokerConfigs.DYNAMICAL_CONFIGS),
                    EditableText.singleLine().build()),
                "ALERT",
                (tables, input, logger) -> {
                  var brokerToAlter =
                      tables.stream()
                          .flatMap(
                              m ->
                                  Optional.ofNullable(m.get(BROKER_ID_KEY))
                                      .map(o -> (Integer) o)
                                      .stream())
                          .collect(Collectors.toSet());
                  if (brokerToAlter.isEmpty()) {
                    logger.log("nothing to alter");
                    return CompletableFuture.completedStage(null);
                  }
                  return context
                      .admin()
                      .brokers()
                      .thenApply(
                          brokers ->
                              brokers.stream()
                                  .filter(b -> brokerToAlter.contains(b.id()))
                                  .collect(Collectors.toList()))
                      .thenCompose(
                          brokers -> {
                            var unsets = input.emptyValueKeys();
                            var sets = input.nonEmptyTexts();
                            if (unsets.isEmpty() && sets.isEmpty()) {
                              logger.log("nothing to alter");
                              return CompletableFuture.completedStage(null);
                            }
                            return FutureUtils.sequence(
                                    brokers.stream()
                                        .flatMap(
                                            broker ->
                                                Stream.of(
                                                    context
                                                        .admin()
                                                        .setConfigs(broker.id(), sets)
                                                        .toCompletableFuture(),
                                                    context
                                                        .admin()
                                                        .unsetConfigs(broker.id(), unsets)
                                                        .toCompletableFuture()))
                                        .collect(Collectors.toList()))
                                .thenAccept(
                                    ignored -> logger.log("succeed to alter: " + brokerToAlter));
                          });
                })
            .build());
  }

  private static Tab folderTab(Context context) {
    BiFunction<Integer, String, Map<String, Object>> metrics =
        (id, path) ->
            context.hasMetrics()
                ? context.clients().entrySet().stream()
                    .filter(e -> e.getKey().id() == id)
                    .findFirst()
                    .map(Map.Entry::getValue)
                    .map(
                        client ->
                            Arrays.stream(LogMetrics.LogCleanerManager.values())
                                .flatMap(
                                    m -> {
                                      try {
                                        return m.fetch(client).stream();
                                      } catch (Exception error) {
                                        return Stream.of();
                                      }
                                    })
                                .filter(m -> m.path().equals(path))
                                .collect(
                                    Collectors.toMap(
                                        LogMetrics.LogCleanerManager.Gauge::metricsName,
                                        m ->
                                            m.type()
                                                    == LogMetrics.LogCleanerManager
                                                        .UNCLEANABLE_BYTES
                                                ? (Object) DataSize.Byte.of(m.value())
                                                : m.value())))
                    .orElse(Map.of())
                : Map.of();

    Supplier<CompletionStage<Node>> nodeSupplier =
        () ->
            context
                .admin()
                .brokers()
                .thenApply(
                    brokers ->
                        PaneBuilder.of()
                            .buttonAction(
                                (input, logger) ->
                                    CompletableFuture.supplyAsync(
                                        () ->
                                            brokers.stream()
                                                .flatMap(
                                                    broker ->
                                                        broker.dataFolders().stream()
                                                            .sorted(
                                                                Comparator.comparing(
                                                                    Broker.DataFolder::path))
                                                            .map(
                                                                d -> {
                                                                  Map<String, Object> result =
                                                                      new LinkedHashMap<>();
                                                                  result.put(
                                                                      "broker id", broker.id());
                                                                  result.put("path", d.path());
                                                                  result.put(
                                                                      "partitions",
                                                                      d.partitionSizes().size());
                                                                  result.put(
                                                                      "size",
                                                                      DataSize.Byte.of(
                                                                          d
                                                                              .partitionSizes()
                                                                              .values()
                                                                              .stream()
                                                                              .mapToLong(s -> s)
                                                                              .sum()));
                                                                  result.put(
                                                                      "orphan partitions",
                                                                      d.orphanPartitionSizes()
                                                                          .size());
                                                                  result.put(
                                                                      "orphan size",
                                                                      DataSize.Byte.of(
                                                                          d
                                                                              .orphanPartitionSizes()
                                                                              .values()
                                                                              .stream()
                                                                              .mapToLong(s -> s)
                                                                              .sum()));
                                                                  result.putAll(
                                                                      metrics.apply(
                                                                          broker.id(), d.path()));
                                                                  return result;
                                                                }))
                                                .collect(Collectors.toList())))
                            .build());
    return Tab.dynamic("folder", nodeSupplier);
  }

  public static Tab of(Context context) {
    return Tab.of(
        "broker",
        TabPane.of(
            Side.TOP,
            List.of(
                basicTab(context), configTab(context), metricsTab(context), folderTab(context))));
  }
}
