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
package org.astraea.common.metrics.broker;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.astraea.common.EnumInfo;
import org.astraea.common.metrics.BeanObject;
import org.astraea.common.metrics.BeanQuery;
import org.astraea.common.metrics.HasBeanObject;
import org.astraea.common.metrics.MBeanClient;

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

    @Override
    public String toString() {
      return alias();
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

    @Override
    public String toString() {
      return alias();
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

    @Override
    public String toString() {
      return alias();
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
      private static final String INCOMING_BYTE_TOTAL = "incoming-byte-total";
      private static final String SELECT_TOTAL = "select-total";
      private static final String SUCCESSFUL_AUTHENTICATION_RATE = "successful-authentication-rate";
      private static final String REAUTHENTICATION_LATENCY_AVG = "reauthentication-latency-avg";
      private static final String NETWORK_IO_RATE = "network-io-rate";
      private static final String CONNECTION_CREATION_TOTAL = "connection-creation-total";
      private static final String SUCCESSFUL_REAUTHENTICATION_RATE =
          "successful-reauthentication-rate";
      private static final String REQUEST_SIZE_MAX = "request-size-max";
      private static final String CONNECTION_CLOSE_RATE = "connection-close-rate";
      private static final String SUCCESSFUL_AUTHENTICATION_TOTAL =
          "successful-authentication-total";
      private static final String IO_TIME_NS_TOTAL = "io-time-ns-total";
      private static final String CONNECTION_COUNT = "connection-count";
      private static final String FAILED_REAUTHENTICATION_TOTAL = "failed-reauthentication-total";
      private static final String REQUEST_RATE = "request-rate";
      private static final String SUCCESSFUL_REAUTHENTICATION_TOTAL =
          "successful-reauthentication-total";
      private static final String RESPONSE_RATE = "response-rate";
      private static final String CONNECTION_CREATION_RATE = "connection-creation-rate";
      private static final String IO_WAIT_TIME_NS_AVG = "io-wait-time-ns-avg";
      private static final String IO_WAIT_TIME_NS_TOTAL = "io-wait-time-ns-total";
      private static final String OUTGOING_BYTE_RATE = "outgoing-byte-rate";
      private static final String IOTIME_TOTAL = "iotime-total";
      private static final String IO_RATIO = "io-ratio";
      private static final String REQUEST_SIZE_AVG = "request-size-avg";
      private static final String OUTGOING_BYTE_TOTAL = "outgoing-byte-total";
      private static final String EXPIRED_CONNECTIONS_KILLED_COUNT =
          "expired-connections-killed-count";
      private static final String CONNECTION_CLOSE_TOTAL = "connection-close-total";
      private static final String FAILED_REAUTHENTICATION_RATE = "failed-reauthentication-rate";
      private static final String NETWORK_IO_TOTAL = "network-io-total";
      private static final String FAILED_AUTHENTICATION_TOTAL = "failed-authentication-total";
      private static final String INCOMING_BYTE_RATE = "incoming-byte-rate";
      private static final String SELECT_RATE = "select-rate";
      private static final String IO_TIME_NS_AVG = "io-time-ns-avg";
      private static final String REAUTHENTICATION_LATENCY_MAX = "reauthentication-latency-max";
      private static final String RESPONSE_TOTAL = "response-total";
      private static final String FAILED_AUTHENTICATION_RATE = "failed-authentication-rate";
      private static final String IO_WAIT_RATIO = "io-wait-ratio";
      private static final String SUCCESSFUL_AUTHENTICATION_NO_REAUTH_TOTAL =
          "successful-authentication-no-reauth-total";
      private static final String REQUEST_TOTAL = "request-total";
      private static final String IO_WAITTIME_TOTAL = "io-waittime-total";

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
        return (double) beanObject().attributes().get(INCOMING_BYTE_TOTAL);
      }

      public double selectTotal() {
        return (double) beanObject().attributes().get(SELECT_TOTAL);
      }

      public double successfulAuthenticationRate() {
        return (double) beanObject().attributes().get(SUCCESSFUL_AUTHENTICATION_RATE);
      }

      public double reauthenticationLatencyAvg() {
        return (double) beanObject().attributes().get(REAUTHENTICATION_LATENCY_AVG);
      }

      public double networkIoRate() {
        return (double) beanObject().attributes().get(NETWORK_IO_RATE);
      }

      public double connectionCreationTotal() {
        return (double) beanObject().attributes().get(CONNECTION_CREATION_TOTAL);
      }

      public double successfulReauthenticationRate() {
        return (double) beanObject().attributes().get(SUCCESSFUL_REAUTHENTICATION_RATE);
      }

      public double requestSizeMax() {
        return (double) beanObject().attributes().get(REQUEST_SIZE_MAX);
      }

      public double connectionCloseRate() {
        return (double) beanObject().attributes().get(CONNECTION_CLOSE_RATE);
      }

      public double successfulAuthenticationTotal() {
        return (double) beanObject().attributes().get(SUCCESSFUL_AUTHENTICATION_TOTAL);
      }

      public double ioTimeNsTotal() {
        return (double) beanObject().attributes().get(IO_TIME_NS_TOTAL);
      }

      public double connectionCount() {
        return (double) beanObject().attributes().get(CONNECTION_COUNT);
      }

      public double failedReauthenticationTotal() {
        return (double) beanObject().attributes().get(FAILED_REAUTHENTICATION_TOTAL);
      }

      public double requestRate() {
        return (double) beanObject().attributes().get(REQUEST_RATE);
      }

      public double successfulReauthenticationTotal() {
        return (double) beanObject().attributes().get(SUCCESSFUL_REAUTHENTICATION_TOTAL);
      }

      public double responseRate() {
        return (double) beanObject().attributes().get(RESPONSE_RATE);
      }

      public double connectionCreationRate() {
        return (double) beanObject().attributes().get(CONNECTION_CREATION_RATE);
      }

      public double ioWaitTimeNsAvg() {
        return (double) beanObject().attributes().get(IO_WAIT_TIME_NS_AVG);
      }

      public double ioWaitTimeNsTotal() {
        return (double) beanObject().attributes().get(IO_WAIT_TIME_NS_TOTAL);
      }

      public double outgoingByteRate() {
        return (double) beanObject().attributes().get(OUTGOING_BYTE_RATE);
      }

      public double iotimeTotal() {
        return (double) beanObject().attributes().get(IOTIME_TOTAL);
      }

      public double ioRatio() {
        return (double) beanObject().attributes().get(IO_RATIO);
      }

      public double requestSizeAvg() {
        return (double) beanObject().attributes().get(REQUEST_SIZE_AVG);
      }

      public double outgoingByteTotal() {
        return (double) beanObject().attributes().get(OUTGOING_BYTE_TOTAL);
      }

      public double expiredConnectionsKilledCount() {
        return (double) beanObject().attributes().get(EXPIRED_CONNECTIONS_KILLED_COUNT);
      }

      public double connectionCloseTotal() {
        return (double) beanObject().attributes().get(CONNECTION_CLOSE_TOTAL);
      }

      public double failedReauthenticationRate() {
        return (double) beanObject().attributes().get(FAILED_REAUTHENTICATION_RATE);
      }

      public double networkIoTotal() {
        return (double) beanObject().attributes().get(NETWORK_IO_TOTAL);
      }

      public double failedAuthenticationTotal() {
        return (double) beanObject().attributes().get(FAILED_AUTHENTICATION_TOTAL);
      }

      public double incomingByteRate() {
        return (double) beanObject().attributes().get(INCOMING_BYTE_RATE);
      }

      public double selectRate() {
        return (double) beanObject().attributes().get(SELECT_RATE);
      }

      public double ioTimeNsAvg() {
        return (double) beanObject().attributes().get(IO_TIME_NS_AVG);
      }

      public double reauthenticationLatencyMax() {
        return (double) beanObject().attributes().get(REAUTHENTICATION_LATENCY_MAX);
      }

      public double responseTotal() {
        return (double) beanObject().attributes().get(RESPONSE_TOTAL);
      }

      public double failedAuthenticationRate() {
        return (double) beanObject().attributes().get(FAILED_AUTHENTICATION_RATE);
      }

      public double ioWaitRatio() {
        return (double) beanObject().attributes().get(IO_WAIT_RATIO);
      }

      public double successfulAuthenticationNoReauthTotal() {
        return (double) beanObject().attributes().get(SUCCESSFUL_AUTHENTICATION_NO_REAUTH_TOTAL);
      }

      public double requestTotal() {
        return (double) beanObject().attributes().get(REQUEST_TOTAL);
      }

      public double ioWaittimeTotal() {
        return (double) beanObject().attributes().get(IO_WAITTIME_TOTAL);
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
