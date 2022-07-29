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
package org.astraea.app.metrics;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;
import org.astraea.app.metrics.broker.HasValue;
import org.astraea.app.metrics.broker.TotalTimeMs;
import org.astraea.app.metrics.producer.HasProducerNodeMetrics;
import org.astraea.app.metrics.producer.HasProducerTopicMetrics;

public final class KafkaMetrics {

  private KafkaMetrics() {}

  public enum Request {
    Produce,
    FetchConsumer,
    FetchFollower;

    public TotalTimeMs totalTimeMs(MBeanClient mBeanClient) {
      return new TotalTimeMs(
          mBeanClient.queryBean(
              BeanQuery.builder()
                  .domainName("kafka.network")
                  .property("type", "RequestMetrics")
                  .property("request", this.name())
                  .property("name", "TotalTimeMs")
                  .build()));
    }
  }

  public enum ReplicaManager {
    AtMinIsrPartitionCount("AtMinIsrPartitionCount"),
    LeaderCount("LeaderCount"),
    OfflineReplicaCount("OfflineReplicaCount"),
    PartitionCount("PartitionCount"),
    ReassigningPartitions("ReassigningPartitions"),
    UnderMinIsrPartitionCount("UnderMinIsrPartitionCount"),
    UnderReplicatedPartitions("UnderReplicatedPartition");
    private final String metricName;

    ReplicaManager(String name) {
      this.metricName = name;
    }

    public String metricName() {
      return metricName;
    }

    public static ReplicaManager of(String metricName) {
      return Arrays.stream(ReplicaManager.values())
          .filter(metric -> metric.metricName().equalsIgnoreCase(metricName))
          .findFirst()
          .orElseThrow(() -> new IllegalArgumentException("No such metric: " + metricName));
    }

    public Collection<HasBeanObject> fetch(MBeanClient mBeanClient) {
      return mBeanClient
          .queryBeans(
              BeanQuery.builder()
                  .domainName("kafka.server")
                  .property("type", "ReplicaManager")
                  .property("name", metricName)
                  .build())
          .stream()
          .map(HasValue::of)
          .collect(Collectors.toUnmodifiableList());
    }
  }

  public static final class Producer {

    private static int brokerId(String node) {
      return Integer.parseInt(node.substring(node.indexOf("-") + 1));
    }

    private Producer() {}

    /**
     * node metrics traced by producer
     *
     * @param mBeanClient to query beans
     * @param brokerId broker ids
     * @return key is client id used by producer, and value is node metrics traced by each producer
     */
    public static Map<String, HasProducerNodeMetrics> node(MBeanClient mBeanClient, int brokerId) {
      return mBeanClient
          .queryBeans(
              BeanQuery.builder()
                  .domainName("kafka.producer")
                  .property("type", "producer-node-metrics")
                  .property("node-id", "node-" + brokerId)
                  .property("client-id", "*")
                  .build())
          .stream()
          .collect(
              Collectors.toUnmodifiableMap(
                  b -> b.properties().get("client-id"),
                  b -> HasProducerNodeMetrics.of(b, brokerId(b.properties().get("node-id")))));
    }

    /**
     * collect HasProducerNodeMetrics from all producers.
     *
     * @param mBeanClient to query metrics
     * @return key is broker id, and value is associated to broker metrics recorded by all producers
     */
    public static Collection<HasProducerNodeMetrics> nodes(MBeanClient mBeanClient) {
      return mBeanClient
          .queryBeans(
              BeanQuery.builder()
                  .domainName("kafka.producer")
                  .property("type", "producer-node-metrics")
                  .property("node-id", "*")
                  .property("client-id", "*")
                  .build())
          .stream()
          .map(b -> HasProducerNodeMetrics.of(b, brokerId(b.properties().get("node-id"))))
          .collect(Collectors.toUnmodifiableList());
    }

    /**
     * topic metrics traced by producer
     *
     * @param mBeanClient to query beans
     * @param topic topic name
     * @return key is client id used by producer, and value is topic metrics traced by each producer
     */
    public static Map<String, HasProducerTopicMetrics> topic(
        MBeanClient mBeanClient, String topic) {
      return mBeanClient
          .queryBeans(
              BeanQuery.builder()
                  .domainName("kafka.producer")
                  .property("type", "producer-topic-metrics")
                  .property("client-id", "*")
                  .property("topic", topic)
                  .build())
          .stream()
          .collect(
              Collectors.toUnmodifiableMap(b -> b.properties().get("client-id"), b -> () -> b));
    }
  }
}
