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

import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.astraea.common.DataSize;
import org.astraea.common.LinkedHashMap;
import org.astraea.common.metrics.MBeanClient;
import org.astraea.common.metrics.broker.ControllerMetrics;
import org.astraea.common.metrics.broker.ServerMetrics;
import org.astraea.common.metrics.platform.HostMetrics;
import org.astraea.gui.Context;
import org.astraea.gui.button.RadioButtonAble;
import org.astraea.gui.pane.PaneBuilder;
import org.astraea.gui.pane.Tab;

public class MetricsTab {

  private static <T> Optional<T> tryToFetch(Supplier<T> function) {
    try {
      return Optional.of(function.get());
    } catch (Exception e) {
      System.out.println(e.getMessage());
      return Optional.empty();
    }
  }

  private enum MetricType implements RadioButtonAble {
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
                .map(m -> tryToFetch(() -> m.fetch(client)))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toMap(m -> m.metricsName(), m -> m.value()))),

    CONTROLLER_STATE(
        "controller state",
        client ->
            Arrays.stream(ControllerMetrics.ControllerState.values())
                .map(m -> tryToFetch(() -> m.fetch(client)))
                .filter(Optional::isPresent)
                .map(Optional::get)
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
                                org.astraea.common.Utils.toSortedMap(
                                    Map.Entry::getKey, Map.Entry::getValue)))
                .orElse(new TreeMap<>())),

    DELAYED_OPERATION(
        "delayed operation",
        client ->
            Arrays.stream(ServerMetrics.DelayedOperationPurgatory.values())
                .map(m -> tryToFetch(() -> m.fetch(client)))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toMap(m -> m.metricsName(), m -> m.value()))),

    REPLICA(
        "replica",
        client ->
            Arrays.stream(ServerMetrics.ReplicaManager.values())
                .map(m -> tryToFetch(() -> m.fetch(client)))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toMap(m -> m.metricsName(), m -> m.value()))),
    BROKER_TOPIC(
        "broker topics",
        client ->
            Arrays.stream(ServerMetrics.BrokerTopic.values())
                .map(m -> tryToFetch(() -> m.fetch(client)))
                .filter(Optional::isPresent)
                .map(Optional::get)
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
    public String display() {
      return display;
    }
  }

  public static Tab of(Context context) {
    var pane =
        PaneBuilder.of()
            .searchField("config key")
            .radioButtons(MetricType.values())
            .buttonAction(
                (input, logger) ->
                    context.metrics(
                        bs ->
                            bs.entrySet().stream()
                                .map(
                                    entry ->
                                        Map.entry(
                                            entry.getKey(),
                                            input
                                                .selectedRadio()
                                                .map(o -> (MetricType) o)
                                                .orElse(MetricType.BROKER_TOPIC)
                                                .fetcher
                                                .apply(entry.getValue())))
                                .sorted(Comparator.comparing(e -> e.getKey().id()))
                                .map(
                                    entry -> {
                                      var result = new LinkedHashMap<String, Object>();
                                      result.put("broker id", entry.getKey().id());
                                      result.put("host", entry.getKey().host());
                                      entry.getValue().entrySet().stream()
                                          .filter(m -> input.matchSearch(m.getKey()))
                                          .forEach(m -> result.put(m.getKey(), m.getValue()));
                                      return result;
                                    })
                                .collect(Collectors.toList())))
            .build();

    return Tab.of("metrics", pane);
  }
}
