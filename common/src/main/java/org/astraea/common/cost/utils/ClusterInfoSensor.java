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
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.astraea.common.admin.ClusterBean;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.NodeInfo;
import org.astraea.common.admin.Replica;
import org.astraea.common.admin.TopicPartition;
import org.astraea.common.metrics.BeanObject;
import org.astraea.common.metrics.BeanQuery;
import org.astraea.common.metrics.HasBeanObject;
import org.astraea.common.metrics.MBeanClient;
import org.astraea.common.metrics.broker.HasGauge;
import org.astraea.common.metrics.broker.LogMetrics;
import org.astraea.common.metrics.collector.MetricSensor;

/** This MetricSensor attempts to reconstruct a ClusterInfo of the kafka cluster via JMX metrics. */
public class ClusterInfoSensor implements MetricSensor {

  @Override
  public List<? extends HasBeanObject> fetch(MBeanClient client, ClusterBean bean) {
    return Stream.of(
            LogMetrics.Log.SIZE.fetch(client), ClusterInfoSensor.ReplicasCountMetric.fetch(client))
        .flatMap(Collection::stream)
        .collect(Collectors.toUnmodifiableList());
  }

  public static ClusterInfo metricViewCluster(ClusterBean clusterBean) {
    var nodes =
        clusterBean.brokerIds().stream()
            .map(id -> NodeInfo.of(id, "", -1))
            .collect(Collectors.toUnmodifiableMap(NodeInfo::id, x -> x));
    var replicas =
        clusterBean.brokerTopics().stream()
            .flatMap(
                (bt) -> {
                  var broker = bt.broker();
                  var partitions =
                      clusterBean
                          .brokerTopicMetrics(bt, ReplicasCountMetric.class)
                          .sorted(
                              Comparator.comparingLong(HasBeanObject::createdTimestamp).reversed())
                          .collect(
                              Collectors.toUnmodifiableMap(
                                  ReplicasCountMetric::topicPartition,
                                  m -> {
                                    var tp = m.topicPartition();
                                    var size =
                                        clusterBean
                                            .brokerMetrics(broker, LogMetrics.Log.Gauge.class)
                                            .filter(x -> x.partition() == tp.partition())
                                            .filter(x -> x.topic().equals(tp.topic()))
                                            .filter(
                                                x ->
                                                    LogMetrics.Log.SIZE
                                                        .metricName()
                                                        .equals(x.metricsName()))
                                            .max(
                                                Comparator.comparingLong(
                                                    HasBeanObject::createdTimestamp))
                                            .orElseThrow()
                                            .value();
                                    var build =
                                        Replica.builder()
                                            .topic(tp.topic())
                                            .partition(tp.partition())
                                            .nodeInfo(nodes.get(broker))
                                            .path("")
                                            .size(size);
                                    return m.isLeader()
                                        ? build.buildLeader()
                                        : build.buildInSyncFollower();
                                  },
                                  (latest, earlier) -> latest));
                  return partitions.values().stream();
                })
            .collect(Collectors.toUnmodifiableList());

    return ClusterInfo.of("", List.copyOf(nodes.values()), Map.of(), replicas);
  }

  public static class ReplicasCountMetric implements HasGauge<Integer> {

    private final BeanObject beanObject;

    ReplicasCountMetric(BeanObject beanObject) {
      this.beanObject = beanObject;
    }

    public static ReplicasCountMetric of(String topic, int partition, int count) {
      return new ReplicasCountMetric(
          new BeanObject(
              "kafka.cluster",
              Map.ofEntries(
                  Map.entry("type", "Partition"),
                  Map.entry("topic", topic),
                  Map.entry("partition", Integer.toString(partition)),
                  Map.entry("name", "ReplicasCount")),
              Map.of("Value", count)));
    }

    static List<ReplicasCountMetric> fetch(MBeanClient client) {
      return client
          .beans(
              BeanQuery.builder()
                  .domainName("kafka.cluster")
                  .property("type", "Partition")
                  .property("topic", "*")
                  .property("partition", "*")
                  .property("name", "ReplicasCount")
                  .build())
          .stream()
          .map(ReplicasCountMetric::new)
          .collect(Collectors.toUnmodifiableList());
    }

    @Override
    public BeanObject beanObject() {
      return beanObject;
    }

    public TopicPartition topicPartition() {
      return TopicPartition.of(
          beanObject.properties().get("topic"),
          Integer.parseInt(beanObject.properties().get("partition")));
    }

    public boolean isLeader() {
      return this.value() != 0;
    }
  }

  static class Builder {
    private String topic;
    private int partition;
    private long size;
  }
}
