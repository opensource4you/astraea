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
package org.astraea.common.cost.utils;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.astraea.common.admin.Broker;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.Replica;
import org.astraea.common.metrics.ClusterBean;
import org.astraea.common.metrics.HasBeanObject;
import org.astraea.common.metrics.broker.ClusterMetrics;
import org.astraea.common.metrics.broker.HasGauge;
import org.astraea.common.metrics.broker.LogMetrics;
import org.astraea.common.metrics.broker.ServerMetrics;
import org.astraea.common.metrics.collector.BeanObjectClient;
import org.astraea.common.metrics.collector.MetricSensor;

/** This MetricSensor attempts to reconstruct a ClusterInfo of the kafka cluster via JMX metrics. */
public class ClusterInfoSensor implements MetricSensor {

  @Override
  public List<? extends HasBeanObject> fetch(BeanObjectClient client, ClusterBean bean) {
    return Stream.of(
            List.of(ServerMetrics.KafkaServer.CLUSTER_ID.fetch(client)),
            LogMetrics.Log.SIZE.fetch(client),
            ClusterMetrics.Partition.REPLICAS_COUNT.fetch(client))
        .flatMap(Collection::stream)
        .toList();
  }

  /**
   * Create a {@link ClusterInfo} from the metrics of a given {@link ClusterBean}. The {@link
   * ClusterInfo} might lack some information due to the incompetent metrics info.
   *
   * @param clusterBean the cluster bean.
   * @throws IllegalStateException when there is some conflict information within the cluster bean
   * @return a {@link ClusterInfo}.
   */
  public static ClusterInfo metricViewCluster(ClusterBean clusterBean) {
    var nodes =
        clusterBean.brokerIds().stream()
            .filter(id -> id != -1)
            .map(id -> Broker.of(id, "", -1))
            .collect(Collectors.toUnmodifiableMap(Broker::id, x -> x));
    var replicaMap =
        clusterBean.brokerIds().stream()
            .collect(
                Collectors.toUnmodifiableMap(
                    broker -> broker,
                    broker ->
                        clusterBean
                            .brokerMetrics(broker, LogMetrics.Log.Gauge.class)
                            .filter(x -> LogMetrics.Log.SIZE.metricName().equals(x.metricsName()))
                            .filter(x -> x.partitionIndex().isPresent())
                            .collect(
                                Collectors.toUnmodifiableMap(
                                    x -> x.partitionIndex().orElseThrow(),
                                    x -> x,
                                    (a, b) ->
                                        a.createdTimestamp() > b.createdTimestamp() ? a : b))));
    var replicas =
        clusterBean.brokerTopics().stream()
            .filter(bt -> bt.broker() != -1)
            .flatMap(
                (bt) -> {
                  var broker = bt.broker();
                  var partitions =
                      clusterBean
                          .brokerTopicMetrics(bt, ClusterMetrics.PartitionMetric.class)
                          .collect(
                              Collectors.toUnmodifiableMap(
                                  ClusterMetrics.PartitionMetric::topicPartition,
                                  x -> x,
                                  (a, b) -> a.createdTimestamp() > b.createdTimestamp() ? a : b))
                          .values()
                          .stream()
                          .collect(
                              Collectors.toUnmodifiableMap(
                                  ClusterMetrics.PartitionMetric::topicPartition,
                                  m -> {
                                    var tp = m.topicPartition();
                                    var size = replicaMap.get(broker).get(tp);
                                    if (size == null)
                                      throw new IllegalStateException(
                                          "Partition "
                                              + tp
                                              + " detected, but its size metric doesn't exists. "
                                              + "Maybe the given cluster bean is partially sampled");
                                    var build =
                                        Replica.builder()
                                            .topic(tp.topic())
                                            .partition(tp.partition())
                                            .broker(nodes.get(broker))
                                            .path("")
                                            .size(size.value());
                                    var isLeader = m.value() != 0;
                                    return isLeader
                                        ? build.buildLeader()
                                        : build.buildInSyncFollower();
                                  }));
                  return partitions.values().stream();
                })
            .toList();
    var clusterId =
        clusterBean.all().entrySet().stream()
            .filter(e -> e.getKey() != -1)
            .map(Map.Entry::getValue)
            .flatMap(Collection::stream)
            .filter(x -> x instanceof ServerMetrics.KafkaServer.ClusterIdGauge)
            .map(x -> (ServerMetrics.KafkaServer.ClusterIdGauge) x)
            .findAny()
            .map(HasGauge::value)
            .orElse("");

    return ClusterInfo.of(clusterId, List.copyOf(nodes.values()), Map.of(), replicas);
  }
}
