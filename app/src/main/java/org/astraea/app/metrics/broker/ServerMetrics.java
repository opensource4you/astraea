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

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.astraea.app.common.Utils;
import org.astraea.app.metrics.BeanObject;
import org.astraea.app.metrics.BeanQuery;
import org.astraea.app.metrics.HasBeanObject;
import org.astraea.app.metrics.MBeanClient;

public final class ServerMetrics {

  public enum KafkaServer {
    YAMMER_METRICS_COUNT("yammer-metrics-count"),
    BROKER_STATE("BrokerState"),
    LINUX_DISK_READ_BYTES("linux-disk-read-bytes"),
    LINUX_DISK_WRITE_BYTES("linux-disk-write-bytes");

    /** Others are Gauge-Number , this is Gauge-String */
    public static final String CLUSTER_ID = "ClusterId";

    private final String metricName;

    public static HasObjectGauge<String> getClientId(MBeanClient mBeanClient) {
      return () ->
          mBeanClient.queryBean(
              BeanQuery.builder()
                  .domainName("kafka.server")
                  .property("type", "KafkaServer")
                  .property("name", CLUSTER_ID)
                  .build());
    }

    KafkaServer(String name) {
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
                  .property("type", "KafkaServer")
                  .property("name", metricName)
                  .build()));
    }

    public static KafkaServer of(String metricName) {
      return Utils.ofIgnoreCaseEnum(KafkaServer.values(), KafkaServer::metricName, metricName);
    }

    public static class Gauge implements HasGauge {
      private final BeanObject beanObject;

      public Gauge(BeanObject beanObject) {
        this.beanObject = beanObject;
      }

      public String metricsName() {
        return beanObject().properties().get("name");
      }

      public DelayedOperationPurgatory type() {
        return DelayedOperationPurgatory.of(metricsName());
      }

      @Override
      public BeanObject beanObject() {
        return beanObject;
      }
    }
  }

  public enum DelayedOperationPurgatory {
    ALTER_ACLS("AlterAcls"),
    DELETE_RECORDS("DeleteRecords"),
    ELECT_LEADER("ElectLeader"),
    FETCH("Fetch"),
    HEARTBEAT("Heartbeat"),
    PRODUCE("Produce"),
    REBALANCE("Rebalance");

    private final String metricName;

    DelayedOperationPurgatory(String name) {
      this.metricName = name;
    }

    public String metricName() {
      return metricName;
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

    public static DelayedOperationPurgatory of(String metricName) {
      return Utils.ofIgnoreCaseEnum(
          DelayedOperationPurgatory.values(), DelayedOperationPurgatory::metricName, metricName);
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
        return DelayedOperationPurgatory.of(metricsName());
      }

      @Override
      public BeanObject beanObject() {
        return beanObject;
      }
    }
  }

  public enum Topic {
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

    private final String metricName;

    Topic(String name) {
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
      return Utils.ofIgnoreCaseEnum(Topic.values(), Topic::metricName, metricName);
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

  public enum ReplicaManager {
    AT_MIN_ISR_PARTITION_COUNT("AtMinIsrPartitionCount"),
    LEADER_COUNT("LeaderCount"),
    OFFLINE_REPLICA_COUNT("OfflineReplicaCount"),
    PARTITION_COUNT("PartitionCount"),
    REASSIGNING_PARTITIONS("ReassigningPartitions"),
    UNDER_MIN_ISR_PARTITION_COUNT("UnderMinIsrPartitionCount"),
    UNDER_REPLICATED_PARTITIONS("UnderReplicatedPartitions");
    private final String metricName;

    ReplicaManager(String name) {
      this.metricName = name;
    }

    public String metricName() {
      return metricName;
    }

    public static ReplicaManager of(String metricName) {
      return Utils.ofIgnoreCaseEnum(
          ReplicaManager.values(), ReplicaManager::metricName, metricName);
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

    public static class Gauge implements HasGauge {

      private final BeanObject beanObject;

      public Gauge(BeanObject beanObject) {
        this.beanObject = Objects.requireNonNull(beanObject);
      }

      public String metricsName() {
        return beanObject().properties().get("name");
      }

      public ReplicaManager type() {
        return ReplicaManager.of(metricsName());
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

  public static class Socket {
    private static final String METRIC_TYPE = "socket-server-metrics";
    private static final String PROP_LISTENER = "listener";
    private static final String PROP_NETWORK_PROCESSOR = "networkProcessor";
    private static final String PROP_CLIENT_SOFTWARE_NAME = "clientSoftwareName";
    private static final String PROP_CLIENT_SOFTWARE_VERSION = "clientSoftwareVersion";

    public static SocketMetric socket(MBeanClient mBeanClient) {
      return new SocketMetric(
          mBeanClient.queryBean(
              BeanQuery.builder()
                  .domainName("kafka.server")
                  .property("type", METRIC_TYPE)
                  .build()));
    }

    public static List<SocketListenerMetric> socketListener(MBeanClient mBeanClient) {
      return mBeanClient
          .queryBeans(
              BeanQuery.builder()
                  .domainName("kafka.server")
                  .property("type", METRIC_TYPE)
                  .property(PROP_LISTENER, "*")
                  .build())
          .stream()
          .map(SocketListenerMetric::new)
          .collect(Collectors.toList());
    }

    public static List<SocketNetworkProcessorMetric> socketNetworkProcessor(
        MBeanClient mBeanClient) {
      return mBeanClient
          .queryBeans(
              BeanQuery.builder()
                  .domainName("kafka.server")
                  .property("type", METRIC_TYPE)
                  .property(PROP_LISTENER, "*")
                  .property(PROP_NETWORK_PROCESSOR, "*")
                  .build())
          .stream()
          .map(SocketNetworkProcessorMetric::new)
          .collect(Collectors.toList());
    }

    public static List<Client> client(MBeanClient mBeanClient) {
      return mBeanClient
          .queryBeans(
              BeanQuery.builder()
                  .domainName("kafka.server")
                  .property("type", METRIC_TYPE)
                  .property(PROP_LISTENER, "*")
                  .property(PROP_NETWORK_PROCESSOR, "*")
                  .property(PROP_CLIENT_SOFTWARE_NAME, "*")
                  .property(PROP_CLIENT_SOFTWARE_VERSION, "*")
                  .build())
          .stream()
          .map(Client::new)
          .collect(Collectors.toList());
    }

    public static class SocketMetric implements HasBeanObject {
      private static final String MEMORY_POOL_DEPLETED_TIME_TOTAL = "MemoryPoolDepletedTimeTotal";
      private static final String MEMORY_POOL_AVG_DEPLETED_PERCENT = "MemoryPoolAvgDepletedPercent";
      private static final String BROKER_CONNECTION_ACCEPT_RATE = "broker-connection-accept-rate";

      private final BeanObject beanObject;

      public SocketMetric(BeanObject beanObject) {
        this.beanObject = Objects.requireNonNull(beanObject);
      }

      @Override
      public BeanObject beanObject() {
        return beanObject;
      }

      public double memoryPoolDepletedTimeTotal() {
        return (double) beanObject().attributes().get(MEMORY_POOL_DEPLETED_TIME_TOTAL);
      }

      public double memoryPoolAvgDepletedPercent() {
        return (double) beanObject().attributes().get(MEMORY_POOL_AVG_DEPLETED_PERCENT);
      }

      public double brokerConnectionAcceptRate() {
        return (double) beanObject().attributes().get(BROKER_CONNECTION_ACCEPT_RATE);
      }
    }

    /** property : listener */
    public static class SocketListenerMetric implements HasBeanObject {
      private static final String CONNECTION_ACCEPT_THROTTLE_TIME =
          "connection-accept-throttle-time";
      private static final String CONNECTION_ACCEPT_RATE = "connection-accept-rate";
      private static final String IP_CONNECTION_ACCEPT_THROTTLE_TIME =
          "ip-connection-accept-throttle-time";

      private final BeanObject beanObject;

      public SocketListenerMetric(BeanObject beanObject) {
        this.beanObject = Objects.requireNonNull(beanObject);
      }

      @Override
      public BeanObject beanObject() {
        return beanObject;
      }

      public String listener() {
        return beanObject().properties().get(PROP_LISTENER);
      }

      public double connectionAcceptThrottleTime() {
        return (double) beanObject().attributes().get(CONNECTION_ACCEPT_THROTTLE_TIME);
      }

      public double connectionAcceptRate() {
        return (double) beanObject().attributes().get(CONNECTION_ACCEPT_RATE);
      }

      public double ipConnectionAcceptThrottleTime() {
        return (double) beanObject().attributes().get(IP_CONNECTION_ACCEPT_THROTTLE_TIME);
      }
    }

    /** property : listener and networkProcessor */
    public static class SocketNetworkProcessorMetric implements HasBeanObject {
      private static final String incomingByteTotal = "incoming-byte-total";
      private static final String selectTotal = "select-total";
      private static final String successfulAuthenticationRate = "successful-authentication-rate";
      private static final String reauthenticationLatencyAvg = "reauthentication-latency-avg";
      private static final String networkIoRate = "network-io-rate";
      private static final String connectionCreationTotal = "connection-creation-total";
      private static final String successfulReauthenticationRate =
          "successful-reauthentication-rate";
      private static final String requestSizeMax = "request-size-max";
      private static final String connectionCloseRate = "connection-close-rate";
      private static final String successfulAuthenticationTotal = "successful-authentication-total";
      private static final String ioTimeNsTotal = "io-time-ns-total";
      private static final String connectionCount = "connection-count";
      private static final String failedReauthenticationTotal = "failed-reauthentication-total";
      private static final String requestRate = "request-rate";
      private static final String successfulReauthenticationTotal =
          "successful-reauthentication-total";
      private static final String responseRate = "response-rate";
      private static final String connectionCreationRate = "connection-creation-rate";
      private static final String ioWaitTimeNsAvg = "io-wait-time-ns-avg";
      private static final String ioWaitTimeNsTotal = "io-wait-time-ns-total";
      private static final String outgoingByteRate = "outgoing-byte-rate";
      private static final String iotimeTotal = "iotime-total";
      private static final String ioRatio = "io-ratio";
      private static final String requestSizeAvg = "request-size-avg";
      private static final String outgoingByteTotal = "outgoing-byte-total";
      private static final String expiredConnectionsKilledCount =
          "expired-connections-killed-count";
      private static final String connectionCloseTotal = "connection-close-total";
      private static final String failedReauthenticationRate = "failed-reauthentication-rate";
      private static final String networkIoTotal = "network-io-total";
      private static final String failedAuthenticationTotal = "failed-authentication-total";
      private static final String incomingByteRate = "incoming-byte-rate";
      private static final String selectRate = "select-rate";
      private static final String ioTimeNsAvg = "io-time-ns-avg";
      private static final String reauthenticationLatencyMax = "reauthentication-latency-max";
      private static final String responseTotal = "response-total";
      private static final String failedAuthenticationRate = "failed-authentication-rate";
      private static final String ioWaitRatio = "io-wait-ratio";
      private static final String successfulAuthenticationNoReauthTotal =
          "successful-authentication-no-reauth-total";
      private static final String requestTotal = "request-total";
      private static final String ioWaittimeTotal = "io-waittime-total";

      private final BeanObject beanObject;

      public SocketNetworkProcessorMetric(BeanObject beanObject) {
        this.beanObject = Objects.requireNonNull(beanObject);
      }

      @Override
      public BeanObject beanObject() {
        return beanObject;
      }

      public String listener() {
        return beanObject().properties().get(PROP_LISTENER);
      }

      public String networkProcessor() {
        return beanObject().properties().get(PROP_NETWORK_PROCESSOR);
      }

      public double incomingByteTotal() {
        return (double) beanObject().attributes().get(incomingByteTotal);
      }

      public double selectTotal() {
        return (double) beanObject().attributes().get(selectTotal);
      }

      public double successfulAuthenticationRate() {
        return (double) beanObject().attributes().get(successfulAuthenticationRate);
      }

      public double reauthenticationLatencyAvg() {
        return (double) beanObject().attributes().get(reauthenticationLatencyAvg);
      }

      public double networkIoRate() {
        return (double) beanObject().attributes().get(networkIoRate);
      }

      public double connectionCreationTotal() {
        return (double) beanObject().attributes().get(connectionCreationTotal);
      }

      public double successfulReauthenticationRate() {
        return (double) beanObject().attributes().get(successfulReauthenticationRate);
      }

      public double requestSizeMax() {
        return (double) beanObject().attributes().get(requestSizeMax);
      }

      public double connectionCloseRate() {
        return (double) beanObject().attributes().get(connectionCloseRate);
      }

      public double successfulAuthenticationTotal() {
        return (double) beanObject().attributes().get(successfulAuthenticationTotal);
      }

      public double ioTimeNsTotal() {
        return (double) beanObject().attributes().get(iotimeTotal);
      }

      public double connectionCount() {
        return (double) beanObject().attributes().get(connectionCount);
      }

      public double failedReauthenticationTotal() {
        return (double) beanObject().attributes().get(failedAuthenticationTotal);
      }

      public double requestRate() {
        return (double) beanObject().attributes().get(requestRate);
      }

      public double successfulReauthenticationTotal() {
        return (double) beanObject().attributes().get(successfulReauthenticationTotal);
      }

      public double responseRate() {
        return (double) beanObject().attributes().get(responseRate);
      }

      public double connectionCreationRate() {
        return (double) beanObject().attributes().get(connectionCreationRate);
      }

      public double ioWaitTimeNsAvg() {
        return (double) beanObject().attributes().get(ioWaitTimeNsAvg);
      }

      public double ioWaitTimeNsTotal() {
        return (double) beanObject().attributes().get(ioWaitTimeNsTotal);
      }

      public double outgoingByteRate() {
        return (double) beanObject().attributes().get(outgoingByteRate);
      }

      public double iotimeTotal() {
        return (double) beanObject().attributes().get(iotimeTotal);
      }

      public double ioRatio() {
        return (double) beanObject().attributes().get(ioRatio);
      }

      public double requestSizeAvg() {
        return (double) beanObject().attributes().get(requestSizeAvg);
      }

      public double outgoingByteTotal() {
        return (double) beanObject().attributes().get(outgoingByteTotal);
      }

      public double expiredConnectionsKilledCount() {
        return (double) beanObject().attributes().get(expiredConnectionsKilledCount);
      }

      public double connectionCloseTotal() {
        return (double) beanObject().attributes().get(connectionCloseTotal);
      }

      public double failedReauthenticationRate() {
        return (double) beanObject().attributes().get(failedReauthenticationRate);
      }

      public double networkIoTotal() {
        return (double) beanObject().attributes().get(networkIoTotal);
      }

      public double failedAuthenticationTotal() {
        return (double) beanObject().attributes().get(failedAuthenticationTotal);
      }

      public double incomingByteRate() {
        return (double) beanObject().attributes().get(incomingByteRate);
      }

      public double selectRate() {
        return (double) beanObject().attributes().get(selectRate);
      }

      public double ioTimeNsAvg() {
        return (double) beanObject().attributes().get(ioTimeNsAvg);
      }

      public double reauthenticationLatencyMax() {
        return (double) beanObject().attributes().get(reauthenticationLatencyMax);
      }

      public double responseTotal() {
        return (double) beanObject().attributes().get(responseTotal);
      }

      public double failedAuthenticationRate() {
        return (double) beanObject().attributes().get(failedAuthenticationRate);
      }

      public double ioWaitRatio() {
        return (double) beanObject().attributes().get(ioWaitRatio);
      }

      public double successfulAuthenticationNoReauthTotal() {
        return (double) beanObject().attributes().get(successfulAuthenticationNoReauthTotal);
      }

      public double requestTotal() {
        return (double) beanObject().attributes().get(requestTotal);
      }

      public double ioWaittimeTotal() {
        return (double) beanObject().attributes().get(ioWaittimeTotal);
      }
    }

    /** property : listener and networkProcessor and clientSoftwareName */
    public static class Client implements HasBeanObject {

      private static final String CONNECTIONS = "connections";
      private final BeanObject beanObject;

      public Client(BeanObject beanObject) {
        this.beanObject = Objects.requireNonNull(beanObject);
      }

      @Override
      public BeanObject beanObject() {
        return beanObject;
      }

      public String listener() {
        return beanObject().properties().get(PROP_LISTENER);
      }

      public String networkProcessor() {
        return beanObject().properties().get(PROP_NETWORK_PROCESSOR);
      }

      public String clientSoftwareName() {
        return beanObject().properties().get(PROP_CLIENT_SOFTWARE_NAME);
      }

      public String clientSoftwareVersion() {
        return beanObject().properties().get(PROP_CLIENT_SOFTWARE_VERSION);
      }

      public int connections() {
        return (int) beanObject().attributes().get(CONNECTIONS);
      }
    }
  }
}
