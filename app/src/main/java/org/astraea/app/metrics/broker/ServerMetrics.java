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
package org.astraea.app.metrics.broker;

import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;
import java.util.stream.Collectors;
import org.astraea.app.common.EnumInfo;
import org.astraea.app.metrics.BeanObject;
import org.astraea.app.metrics.BeanQuery;
import org.astraea.app.metrics.HasBeanObject;
import org.astraea.app.metrics.MBeanClient;

public final class ServerMetrics {

  public enum DelayedOperationPurgatory implements EnumInfo {
    ALTER_ACLS("AlterAcls"),
    DELETE_RECORDS("DeleteRecords"),
    ELECT_LEADER("ElectLeader"),
    FETCH("Fetch"),
    HEARTBEAT("Heartbeat"),
    PRODUCE("Produce"),
    REBALANCE("Rebalance");

    public static DelayedOperationPurgatory ofAlias(String alias) {
      return EnumInfo.ignoreCaseEnum(DelayedOperationPurgatory.class, alias);
    }

    private final String metricName;

    DelayedOperationPurgatory(String name) {
      this.metricName = name;
    }

    public String metricName() {
      return metricName;
    }

    @Override
    public String alias() {
      return metricName();
    }

    public Collection<Gauge> fetch(MBeanClient mBeanClient) {
      return mBeanClient
          .queryBeans(
              BeanQuery.builder()
                  .domainName("kafka.server")
                  .property("type", "DelayedOperationPurgatory")
                  .property("delayedOperation", metricName)
                  .property("name", "PurgatorySize")
                  .build())
          .stream()
          .map(Gauge::new)
          .collect(Collectors.toUnmodifiableList());
    }

    public static class Gauge implements HasGauge {
      private final BeanObject beanObject;

      public Gauge(BeanObject beanObject) {
        this.beanObject = beanObject;
      }

      public String metricsName() {
        return beanObject().properties().get("delayedOperation");
      }

      public DelayedOperationPurgatory type() {
        return DelayedOperationPurgatory.ofAlias(metricsName());
      }

      @Override
      public BeanObject beanObject() {
        return beanObject;
      }
    }
  }

  public enum Topic implements EnumInfo {
    /** Message validation failure rate due to non-continuous offset or sequence number in batch */
    INVALID_OFFSET_OR_SEQUENCE_RECORDS_PER_SEC("InvalidOffsetOrSequenceRecordsPerSec"),

    /** Message validation failure rate due to incorrect crc checksum */
    INVALID_MESSAGE_CRC_RECORDS_PER_SEC("InvalidMessageCrcRecordsPerSec"),

    FETCH_MESSAGE_CONVERSIONS_PER_SEC("FetchMessageConversionsPerSec"),

    BYTES_REJECTED_PER_SEC("BytesRejectedPerSec"),

    /** Message in rate */
    MESSAGES_IN_PER_SEC("MessagesInPerSec"),

    /** Incoming byte rate of reassignment traffic */
    REASSIGNMENT_BYTES_IN_PER_SEC("ReassignmentBytesInPerSec"),

    FAILED_FETCH_REQUESTS_PER_SEC("FailedFetchRequestsPerSec"),

    /** Byte in rate from other brokers */
    REPLICATION_BYTES_IN_PER_SEC("ReplicationBytesInPerSec"),

    /** Message validation failure rate due to no key specified for compacted topic */
    NO_KEY_COMPACTED_TOPIC_RECORDS_PER_SEC("NoKeyCompactedTopicRecordsPerSec"),

    TOTAL_FETCH_REQUESTS_PER_SEC("TotalFetchRequestsPerSec"),

    FAILED_PRODUCE_REQUESTS_PER_SEC("FailedProduceRequestsPerSec"),

    /** Byte in rate from clients */
    BYTES_IN_PER_SEC("BytesInPerSec"),

    TOTAL_PRODUCE_REQUESTS_PER_SEC("TotalProduceRequestsPerSec"),

    /** Message validation failure rate due to invalid magic number */
    INVALID_MAGIC_NUMBER_RECORDS_PER_SEC("InvalidMagicNumberRecordsPerSec"),

    /** Outgoing byte rate of reassignment traffic */
    REASSIGNMENT_BYTES_OUT_PER_SEC("ReassignmentBytesOutPerSec"),

    /** Bytes in rate from other brokers */
    REPLICATION_BYTES_OUT_PER_SEC("ReplicationBytesOutPerSec"),

    PRODUCE_MESSAGE_CONVERSIONS_PER_SEC("ProduceMessageConversionsPerSec"),

    /** Byte out rate to clients. */
    BYTES_OUT_PER_SEC("BytesOutPerSec");

    public static Topic ofAlias(String alias) {
      return EnumInfo.ignoreCaseEnum(Topic.class, alias);
    }

    private final String metricName;

    Topic(String name) {
      this.metricName = name;
    }

    public String metricName() {
      return metricName;
    }

    @Override
    public String alias() {
      return metricName();
    }

    /**
     * find out the objects related to this metrics.
     *
     * @param objects to search
     * @return collection of BrokerTopicMetricsResult, or empty if all objects are not related to
     *     this metrics
     */
    public Collection<Meter> of(Collection<HasBeanObject> objects) {
      return objects.stream()
          .filter(o -> o instanceof Meter)
          .filter(o -> metricName().equals(o.beanObject().properties().get("name")))
          .map(o -> (Meter) o)
          .collect(Collectors.toUnmodifiableList());
    }

    public Meter fetch(MBeanClient mBeanClient) {
      return new Meter(
          mBeanClient.queryBean(
              BeanQuery.builder()
                  .domainName("kafka.server")
                  .property("type", "BrokerTopicMetrics")
                  .property("name", this.metricName())
                  .build()));
    }

    /**
     * resolve specific {@link Topic} by the given metric string, compare by case-insensitive
     *
     * @param metricName the metric to resolve
     * @return a {@link Topic} match to give metric name
     */
    public static Topic of(String metricName) {
      return Arrays.stream(Topic.values())
          .filter(metric -> metric.metricName().equalsIgnoreCase(metricName))
          .findFirst()
          .orElseThrow(() -> new IllegalArgumentException("No such metric: " + metricName));
    }

    public static class Meter implements HasMeter {

      private final BeanObject beanObject;

      public Meter(BeanObject beanObject) {
        this.beanObject = Objects.requireNonNull(beanObject);
      }

      public String metricsName() {
        return beanObject().properties().get("name");
      }

      public Topic type() {
        return Topic.of(metricsName());
      }

      @Override
      public String toString() {
        return beanObject().toString();
      }

      @Override
      public BeanObject beanObject() {
        return beanObject;
      }
    }
  }

  public enum ReplicaManager implements EnumInfo {
    AT_MIN_ISR_PARTITION_COUNT("AtMinIsrPartitionCount"),
    LEADER_COUNT("LeaderCount"),
    OFFLINE_REPLICA_COUNT("OfflineReplicaCount"),
    PARTITION_COUNT("PartitionCount"),
    REASSIGNING_PARTITIONS("ReassigningPartitions"),
    UNDER_MIN_ISR_PARTITION_COUNT("UnderMinIsrPartitionCount"),
    UNDER_REPLICATED_PARTITIONS("UnderReplicatedPartitions");

    public static ReplicaManager ofAlias(String alias) {
      return EnumInfo.ignoreCaseEnum(ReplicaManager.class, alias);
    }

    private final String metricName;

    ReplicaManager(String name) {
      this.metricName = name;
    }

    public String metricName() {
      return metricName;
    }

    public Gauge fetch(MBeanClient mBeanClient) {
      return new Gauge(
          mBeanClient.queryBean(
              BeanQuery.builder()
                  .domainName("kafka.server")
                  .property("type", "ReplicaManager")
                  .property("name", metricName)
                  .build()));
    }

    @Override
    public String alias() {
      return metricName();
    }

    public static class Gauge implements HasGauge {

      private final BeanObject beanObject;

      public Gauge(BeanObject beanObject) {
        this.beanObject = Objects.requireNonNull(beanObject);
      }

      public String metricsName() {
        return beanObject().properties().get("name");
      }

      public ReplicaManager type() {
        return ReplicaManager.ofAlias(metricsName());
      }

      @Override
      public String toString() {
        return beanObject().toString();
      }

      @Override
      public BeanObject beanObject() {
        return beanObject;
      }
    }
  }
}
