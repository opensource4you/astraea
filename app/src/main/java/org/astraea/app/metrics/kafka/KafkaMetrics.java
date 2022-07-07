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
package org.astraea.app.metrics.kafka;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;
import org.astraea.app.metrics.HasBeanObject;
import org.astraea.app.metrics.java.JvmMemory;
import org.astraea.app.metrics.java.OperatingSystemInfo;
import org.astraea.app.metrics.jmx.BeanObject;
import org.astraea.app.metrics.jmx.BeanQuery;
import org.astraea.app.metrics.jmx.MBeanClient;
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
              .getAttributes()
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
              .getAttributes()
              .get("Value");
    }
  }

  public enum Purgatory {
    AlterAcls("AlterAcls"),
    DeleteRecords("DeleteRecords"),
    ElectLeader("ElectLeader"),
    Fetch("Fetch"),
    Heartbeat("Heartbeat"),
    Produce("Produce"),
    Rebalance("Rebalance");

    private final String metricName;

    Purgatory(String name) {
      this.metricName = name;
    }

    public String metricName() {
      return metricName;
    }

    public Collection<HasBeanObject> fetch(MBeanClient mBeanClient) {
      return mBeanClient
          .queryBeans(
              BeanQuery.builder()
                  .domainName("kafka.server")
                  .property("type", "DelayedOperationPurgatory")
                  .property("delayedOperation", metricName)
                  .property("name", "PurgatorySize")
                  .build())
          .stream()
          .map(HasValue::of)
          .collect(Collectors.toUnmodifiableList());
    }

    public int size(MBeanClient mBeanClient) {
      return (int)
          mBeanClient
              .queryBean(
                  BeanQuery.builder()
                      .domainName("kafka.server")
                      .property("type", "DelayedOperationPurgatory")
                      .property("delayedOperation", this.name())
                      .property("name", "PurgatorySize")
                      .build())
              .getAttributes()
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

  public enum TopicPartition {
    LogEndOffset("LogEndOffset"),
    LogStartOffset("LogStartOffset"),
    NumLogSegments("NumLogSegments"),
    Size("Size");
    private final String metricName;

    TopicPartition(String name) {
      this.metricName = name;
    }

    public String metricName() {
      return metricName;
    }

    public static TopicPartition of(String metricName) {
      return Arrays.stream(TopicPartition.values())
          .filter(metric -> metric.metricName().equalsIgnoreCase(metricName))
          .findFirst()
          .orElseThrow(() -> new IllegalArgumentException("No such metric: " + metricName));
    }

    public Collection<HasBeanObject> fetch(MBeanClient mBeanClient) {
      return mBeanClient
          .queryBeans(
              BeanQuery.builder()
                  .domainName("kafka.log")
                  .property("type", "Log")
                  .property("topic", "*")
                  .property("partition", "*")
                  .property("name", metricName)
                  .build())
          .stream()
          .map(HasValue::of)
          .collect(Collectors.toUnmodifiableList());
    }

    /**
     * Number of partitions across all topics in the cluster.
     *
     * @return number of partitions across all topics in the cluster.
     */
    public static int globalPartitionCount(MBeanClient mBeanClient) {
      return (int)
          mBeanClient
              .queryBean(
                  BeanQuery.builder()
                      .domainName("kafka.controller")
                      .property("type", "KafkaController")
                      .property("name", "GlobalPartitionCount")
                      .build())
              .getAttributes()
              .get("Value");
    }

    /**
     * Number of under-replicated partitions (| ISR | < | current replicas |). Replicas that are
     * added as part of a reassignment will not count toward this value. Alert if value is greater
     * than 0.
     *
     * @return number of under-replicated partitions.
     */
    public static int underReplicatedPartitions(MBeanClient mBeanClient) {
      return (int)
          mBeanClient
              .queryBean(
                  BeanQuery.builder()
                      .domainName("kafka.server")
                      .property("type", "ReplicaManager")
                      .property("name", "UnderReplicatedPartitions")
                      .build())
              .getAttributes()
              .get("Value");
    }

    /**
     * retrieve the log size of partitions under specific topic in specific broker.
     *
     * @param client a {@link MBeanClient} instance connect to specific kafka broker
     * @param topicName the name of the topic to query
     * @return a {@link Map} of ({@link Integer}, {@link Long}) pairs that each entry represent a
     *     pair of partition id and its log size
     */
    public static Map<Integer, Long> size(MBeanClient client, String topicName) {
      return client
          .queryBeans(
              BeanQuery.builder()
                  .domainName("kafka.log")
                  .property("type", "Log")
                  .property("topic", topicName)
                  .property("partition", "*")
                  .property("name", "Size")
                  .build())
          .stream()
          .collect(
              Collectors.toMap(
                  (BeanObject a) -> Integer.parseInt(a.getProperties().get("partition")),
                  (BeanObject a) -> (Long) a.getAttributes().get("Value")));
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
              Collectors.toUnmodifiableMap(b -> b.getProperties().get("client-id"), b -> () -> b));
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
              Collectors.toUnmodifiableMap(b -> b.getProperties().get("client-id"), b -> () -> b));
    }
  }
}
