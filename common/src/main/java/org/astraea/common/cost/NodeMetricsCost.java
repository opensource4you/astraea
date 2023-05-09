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
package org.astraea.common.cost;

import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.NodeInfo;
import org.astraea.common.metrics.ClusterBean;
import org.astraea.common.metrics.client.HasNodeMetrics;
import org.astraea.common.metrics.client.producer.ProducerMetrics;
import org.astraea.common.metrics.collector.MetricSensor;

/** Calculate the cost by client-node-metrics. */
public abstract class NodeMetricsCost implements HasBrokerCost {
  @Override
  public BrokerCost brokerCost(ClusterInfo clusterInfo, ClusterBean clusterBean) {
    var result =
        new HashMap<>(
            clusterBean.all().values().stream()
                .flatMap(Collection::stream)
                .filter(b -> b instanceof HasNodeMetrics)
                .map(b -> (HasNodeMetrics) b)
                .filter(b -> !Double.isNaN(value(b)))
                .collect(Collectors.groupingBy(HasNodeMetrics::brokerId))
                .entrySet()
                .stream()
                .collect(
                    Collectors.toMap(
                        Map.Entry::getKey,
                        e ->
                            e.getValue().stream()
                                .sorted(
                                    Comparator.comparing(HasNodeMetrics::createdTimestamp)
                                        .reversed())
                                .limit(1)
                                .mapToDouble(this::value)
                                .sum())));
    // Set max cost for the nodes having no metrics
    result.values().stream()
        .mapToDouble(v -> v)
        .max()
        .ifPresent(
            max ->
                clusterInfo.nodes().stream()
                    .map(NodeInfo::id)
                    .filter(id -> !result.containsKey(id))
                    .forEach(id -> result.put(id, max)));
    return () -> result;
  }

  /** The metrics to take into consider. */
  protected abstract double value(HasNodeMetrics hasNodeMetrics);

  @Override
  public MetricSensor metricSensor() {
    return (client, clusterBean) -> ProducerMetrics.node(client);
  }
}
