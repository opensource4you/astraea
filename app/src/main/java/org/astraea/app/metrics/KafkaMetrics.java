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
import java.util.function.Function;
import java.util.stream.Collectors;
import org.astraea.app.metrics.broker.BrokerTopicMetricsResult;
import org.astraea.app.metrics.broker.HasValue;
import org.astraea.app.metrics.broker.TotalTimeMs;
import org.astraea.app.metrics.platform.JvmMemory;
import org.astraea.app.metrics.platform.OperatingSystemInfo;
import org.astraea.app.metrics.producer.HasProducerNodeMetrics;
import org.astraea.app.metrics.producer.HasProducerTopicMetrics;

public final class KafkaMetrics {

  private KafkaMetrics() {}

  public enum BrokerTopic {
    /** Message validation failure rate due to non-continuous offset or sequence number in batch */
    InvalidOffsetOrSequenceRecordsPerSec("InvalidOffsetOrSequenceRecordsPerSec"),

    /** Message validation failure rate due to incorrect crc checksum */
    InvalidMessageCrcRecordsPerSec("InvalidMessageCrcRecordsPerSec"),

    FetchMessageConversionsPerSec("FetchMessageConversionsPerSec"),

    BytesRejectedPerSec("BytesRejectedPerSec"),

    /** Message in rate */
    MessagesInPerSec("MessagesInPerSec"),

    /** Incoming byte rate of reassignment traffic */
    ReassignmentBytesInPerSec("ReassignmentBytesInPerSec"),

    FailedFetchRequestsPerSec("FailedFetchRequestsPerSec"),

    /** Byte in rate from other brokers */
    ReplicationBytesInPerSec("ReplicationBytesInPerSec"),

    /** Message validation failure rate due to no key specified for compacted topic */
    NoKeyCompactedTopicRecordsPerSec("NoKeyCompactedTopicRecordsPerSec"),

    TotalFetchRequestsPerSec("TotalFetchRequestsPerSec"),

    FailedProduceRequestsPerSec("FailedProduceRequestsPerSec"),

    /** Byte in rate from clients */
    BytesInPerSec("BytesInPerSec"),

    TotalProduceRequestsPerSec("TotalProduceRequestsPerSec"),

    /** Message validation failure rate due to invalid magic number */
    InvalidMagicNumberRecordsPerSec("InvalidMagicNumberRecordsPerSec"),

    /** Outgoing byte rate of reassignment traffic */
    ReassignmentBytesOutPerSec("ReassignmentBytesOutPerSec"),

    /** Bytes in rate from other brokers */
    ReplicationBytesOutPerSec("ReplicationBytesOutPerSec"),

    ProduceMessageConversionsPerSec("ProduceMessageConversionsPerSec"),

    /** Byte out rate to clients. */
    BytesOutPerSec("BytesOutPerSec");

    private final String metricName;

    BrokerTopic(String name) {
      this.metricName = name;
    }

    public String metricName() {
      return metricName;
    }

    /**
     * find out the objects related to this metrics.
     *
     * @param objects to search
     * @return collection of BrokerTopicMetricsResult, or empty if all objects are not related to
     *     this metrics
     */
    public Collection<BrokerTopicMetricsResult> of(Collection<HasBeanObject> objects) {
      return objects.stream()
          .filter(o -> o instanceof BrokerTopicMetricsResult)
          .filter(o -> metricName().equals(o.beanObject().properties().get("name")))
          .map(o -> (BrokerTopicMetricsResult) o)
          .collect(Collectors.toUnmodifiableList());
    }

    public BrokerTopicMetricsResult fetch(MBeanClient mBeanClient) {
      return new BrokerTopicMetricsResult(
          mBeanClient.queryBean(
              BeanQuery.builder()
                  .domainName("kafka.server")
                  .property("type", "BrokerTopicMetrics")
                  .property("name", this.metricName())
                  .build()));
    }

    /**
     * resolve specific {@link BrokerTopic} by the given metric string, compare by case insensitive
     *
     * @param metricName the metric to resolve
     * @return a {@link BrokerTopic} match to give metric name
     */
    public static BrokerTopic of(String metricName) {
      return Arrays.stream(BrokerTopic.values())
          .filter(metric -> metric.metricName().equalsIgnoreCase(metricName))
          .findFirst()
          .orElseThrow(() -> new IllegalArgumentException("No such metric: " + metricName));
    }

    public static long linuxDiskReadBytes(MBeanClient mBeanClient) {
      return (long)
          mBeanClient
              .queryBean(
                  BeanQuery.builder()
                      .domainName("kafka.server")
                      .property("type", "KafkaServer")
                      .property("name", "linux-disk-read-bytes")
                      .build())
              .attributes()
              .get("Value");
    }

    public static long linuxDiskWriteBytes(MBeanClient mBeanClient) {
      return (long)
          mBeanClient
              .queryBean(
                  BeanQuery.builder()
                      .domainName("kafka.server")
                      .property("type", "KafkaServer")
                      .property("name", "linux-disk-write-bytes")
                      .build())
              .attributes()
              .get("Value");
    }
  }

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

  public static final class Host {

    private Host() {}

    /**
     * retrieve broker's operating system info
     *
     * @param mBeanClient a {@link MBeanClient} instance connect to specific kafka broker
     * @return a {@link OperatingSystemInfo} describe broker's os info (arch, processors, memory,
     *     cpu loading ...)
     */
    public static OperatingSystemInfo operatingSystem(MBeanClient mBeanClient) {
      return new OperatingSystemInfo(
          mBeanClient.queryBean(
              BeanQuery.builder()
                  .domainName("java.lang")
                  .property("type", "OperatingSystem")
                  .build()));
    }

    public static JvmMemory jvmMemory(MBeanClient mBeanClient) {
      return new JvmMemory(
          mBeanClient.queryBean(
              BeanQuery.builder().domainName("java.lang").property("type", "Memory").build()));
    }
  }

  public static final class Producer {

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
              Collectors.toUnmodifiableMap(b -> b.properties().get("client-id"), b -> () -> b));
    }

    /**
     * collect HasProducerNodeMetrics from all producers.
     *
     * @param mBeanClient to query metrics
     * @return key is broker id, and value is associated to broker metrics recorded by all producers
     */
    public static Map<Integer, Collection<HasProducerNodeMetrics>> nodes(MBeanClient mBeanClient) {
      Function<String, Integer> brokerId =
          string -> Integer.parseInt(string.substring(string.indexOf("-") + 1));
      return mBeanClient
          .queryBeans(
              BeanQuery.builder()
                  .domainName("kafka.producer")
                  .property("type", "producer-node-metrics")
                  .property("node-id", "*")
                  .property("client-id", "*")
                  .build())
          .stream()
          .collect(Collectors.groupingBy(b -> brokerId.apply(b.properties().get("node-id"))))
          .entrySet()
          .stream()
          .collect(
              Collectors.toMap(
                  Map.Entry::getKey,
                  e ->
                      e.getValue().stream()
                          .map(b -> (HasProducerNodeMetrics) (() -> b))
                          .collect(Collectors.toUnmodifiableList())));
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
