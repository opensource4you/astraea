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

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.astraea.common.EnumInfo;
import org.astraea.common.Utils;
import org.astraea.common.metrics.AppInfo;
import org.astraea.common.metrics.BeanObject;
import org.astraea.common.metrics.BeanQuery;
import org.astraea.common.metrics.HasBeanObject;
import org.astraea.common.metrics.MBeanClient;

public final class ServerMetrics {
  public static final String DOMAIN_NAME = "kafka.server";

  public static final BeanQuery APP_INFO_QUERY =
      BeanQuery.builder()
          .domainName(DOMAIN_NAME)
          .property("type", "app-info")
          .property("id", "*")
          .build();

  public static final Collection<BeanQuery> QUERIES =
      Stream.of(
              Stream.of(APP_INFO_QUERY),
              KafkaServer.ALL.values().stream(),
              DelayedOperationPurgatory.ALL.values().stream(),
              Topic.ALL.values().stream(),
              BrokerTopic.ALL.values().stream(),
              ReplicaManager.ALL.values().stream(),
              Socket.QUERIES.stream())
          .flatMap(f -> f)
          .toList();

  public static List<AppInfo> appInfo(MBeanClient client) {
    return client.beans(APP_INFO_QUERY).stream()
        .map(b -> (AppInfo) () -> b)
        .collect(Collectors.toList());
  }

  public enum KafkaServer implements EnumInfo {
    CLUSTER_ID("ClusterId"),
    YAMMER_METRICS_COUNT("yammer-metrics-count"),
    BROKER_STATE("BrokerState"),
    LINUX_DISK_READ_BYTES("linux-disk-read-bytes"),
    LINUX_DISK_WRITE_BYTES("linux-disk-write-bytes");

    /** Others are Gauge-Number , this is Gauge-String */
    private final String metricName;

    private static final Map<KafkaServer, BeanQuery> ALL =
        Arrays.stream(KafkaServer.values())
            .collect(
                Collectors.toUnmodifiableMap(
                    Function.identity(),
                    m ->
                        BeanQuery.builder()
                            .domainName(DOMAIN_NAME)
                            .property("type", "KafkaServer")
                            .property("name", m.metricName)
                            .build()));

    KafkaServer(String name) {
      this.metricName = name;
    }

    public String metricName() {
      return metricName;
    }

    public HasBeanObject fetch(MBeanClient mBeanClient) {
      if (this == KafkaServer.CLUSTER_ID)
        return new ClusterIdGauge(mBeanClient.bean(ALL.get(this)));

      return new Gauge(mBeanClient.bean(ALL.get(this)));
    }

    public static KafkaServer ofAlias(String alias) {
      return EnumInfo.ignoreCaseEnum(KafkaServer.class, alias);
    }

    @Override
    public String alias() {
      return metricName();
    }

    @Override
    public String toString() {
      return alias();
    }

    public record Gauge(BeanObject beanObject) implements HasGauge<Long> {

      public String metricsName() {
        return beanObject().properties().get("name");
      }

      public KafkaServer type() {
        return ofAlias(metricsName());
      }
    }

    public record ClusterIdGauge(BeanObject beanObject) implements HasGauge<String> {}
  }

  public enum DelayedOperationPurgatory implements EnumInfo {
    ALTER_ACLS("AlterAcls"),
    DELETE_RECORDS("DeleteRecords"),
    ELECT_LEADER("ElectLeader"),
    FETCH("Fetch"),
    HEARTBEAT("Heartbeat"),
    PRODUCE("Produce"),
    REBALANCE("Rebalance");

    private static final Map<DelayedOperationPurgatory, BeanQuery> ALL =
        Arrays.stream(DelayedOperationPurgatory.values())
            .collect(
                Collectors.toUnmodifiableMap(
                    Function.identity(),
                    m ->
                        BeanQuery.builder()
                            .domainName(DOMAIN_NAME)
                            .property("type", "DelayedOperationPurgatory")
                            .property("delayedOperation", m.metricName)
                            .property("name", "PurgatorySize")
                            .build()));

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

    public Gauge fetch(MBeanClient mBeanClient) {
      return new Gauge(mBeanClient.bean(ALL.get(this)));
    }

    public record Gauge(BeanObject beanObject) implements HasGauge<Integer> {

      public String metricsName() {
        return beanObject().properties().get("delayedOperation");
      }

      public DelayedOperationPurgatory type() {
        return ofAlias(metricsName());
      }
    }
  }

  public enum Topic implements EnumInfo {
    BYTES_IN_PER_SEC("BytesInPerSec"),
    BYTES_OUT_PER_SEC("BytesOutPerSec"),
    MESSAGES_IN_PER_SEC("MessagesInPerSec"),
    TOTAL_FETCH_REQUESTS_PER_SEC("TotalFetchRequestsPerSec"),
    TOTAL_PRODUCE_REQUESTS_PER_SEC("TotalProduceRequestsPerSec");

    private static final Map<Topic, BeanQuery> ALL =
        Arrays.stream(Topic.values())
            .collect(
                Collectors.toUnmodifiableMap(
                    Function.identity(),
                    m ->
                        BeanQuery.builder()
                            .domainName(DOMAIN_NAME)
                            .property("type", "BrokerTopicMetrics")
                            .property("topic", "*")
                            .property("name", m.metricName())
                            .build()));

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

    public List<Topic.Meter> fetch(MBeanClient mBeanClient) {
      return mBeanClient.beans(ALL.get(this)).stream()
          .map(Topic.Meter::new)
          .collect(Collectors.toList());
    }

    public record Meter(BeanObject beanObject) implements HasMeter {
      public String metricsName() {
        return beanObject().properties().get("name");
      }

      public String topic() {
        return beanObject().properties().get("topic");
      }

      public Topic type() {
        return ofAlias(metricsName());
      }
    }
  }

  public enum BrokerTopic implements EnumInfo {
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

    private static final Map<BrokerTopic, BeanQuery> ALL =
        Arrays.stream(BrokerTopic.values())
            .collect(
                Collectors.toUnmodifiableMap(
                    Function.identity(),
                    m ->
                        BeanQuery.builder()
                            .domainName(DOMAIN_NAME)
                            .property("type", "BrokerTopicMetrics")
                            .property("name", m.metricName())
                            .build()));

    public static BrokerTopic ofAlias(String alias) {
      return EnumInfo.ignoreCaseEnum(BrokerTopic.class, alias);
    }

    private final String metricName;

    BrokerTopic(String name) {
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
          .toList();
    }

    public Meter fetch(MBeanClient mBeanClient) {
      return new Meter(mBeanClient.bean(ALL.get(this)));
    }

    public record Meter(BeanObject beanObject) implements HasMeter {

      public String metricsName() {
        return beanObject().properties().get("name");
      }

      public BrokerTopic type() {
        return ofAlias(metricsName());
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

    private static final Map<ReplicaManager, BeanQuery> ALL =
        Arrays.stream(ReplicaManager.values())
            .collect(
                Collectors.toUnmodifiableMap(
                    Function.identity(),
                    m ->
                        BeanQuery.builder()
                            .domainName(DOMAIN_NAME)
                            .property("type", "ReplicaManager")
                            .property("name", m.metricName)
                            .build()));

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
      return new Gauge(mBeanClient.bean(ALL.get(this)));
    }

    @Override
    public String alias() {
      return metricName();
    }

    @Override
    public String toString() {
      return alias();
    }

    public record Gauge(BeanObject beanObject) implements HasGauge<Integer> {
      public String metricsName() {
        return beanObject().properties().get("name");
      }

      public ReplicaManager type() {
        return ReplicaManager.ofAlias(metricsName());
      }
    }
  }

  public static class Socket {
    private static final String METRIC_TYPE = "socket-server-metrics";
    private static final String PROP_LISTENER = "listener";
    private static final String PROP_NETWORK_PROCESSOR = "networkProcessor";
    private static final String PROP_CLIENT_SOFTWARE_NAME = "clientSoftwareName";
    private static final String PROP_CLIENT_SOFTWARE_VERSION = "clientSoftwareVersion";

    public static final BeanQuery SOCKET_LISTENER_QUERY =
        BeanQuery.builder()
            .domainName(DOMAIN_NAME)
            .property("type", METRIC_TYPE)
            .property(PROP_LISTENER, "*")
            .build();

    public static final BeanQuery SOCKET_NETWORK_PROCESSOR_QUERY =
        BeanQuery.builder()
            .domainName(DOMAIN_NAME)
            .property("type", METRIC_TYPE)
            .property(PROP_LISTENER, "*")
            .property(PROP_NETWORK_PROCESSOR, "*")
            .build();

    public static final BeanQuery CLIENT_QUERY =
        BeanQuery.builder()
            .domainName(DOMAIN_NAME)
            .property("type", METRIC_TYPE)
            .property(PROP_LISTENER, "*")
            .property(PROP_NETWORK_PROCESSOR, "*")
            .property(PROP_CLIENT_SOFTWARE_NAME, "*")
            .property(PROP_CLIENT_SOFTWARE_VERSION, "*")
            .build();

    public static final BeanQuery SOCKET_QUERY =
        BeanQuery.builder().domainName(DOMAIN_NAME).property("type", METRIC_TYPE).build();

    public static final Collection<BeanQuery> QUERIES =
        Utils.constants(Socket.class, name -> name.endsWith("QUERY"), BeanQuery.class);

    public static SocketMetric socket(MBeanClient mBeanClient) {
      return new SocketMetric(mBeanClient.bean(SOCKET_QUERY));
    }

    public static List<SocketListenerMetric> socketListener(MBeanClient mBeanClient) {
      return mBeanClient.beans(SOCKET_LISTENER_QUERY).stream()
          .map(SocketListenerMetric::new)
          .collect(Collectors.toList());
    }

    public static List<SocketNetworkProcessorMetric> socketNetworkProcessor(
        MBeanClient mBeanClient) {
      return mBeanClient.beans(SOCKET_NETWORK_PROCESSOR_QUERY).stream()
          .map(SocketNetworkProcessorMetric::new)
          .collect(Collectors.toList());
    }

    public static List<Client> client(MBeanClient mBeanClient) {
      return mBeanClient.beans(CLIENT_QUERY).stream().map(Client::new).collect(Collectors.toList());
    }

    public record SocketMetric(BeanObject beanObject) implements HasBeanObject {
      private static final String MEMORY_POOL_DEPLETED_TIME_TOTAL = "MemoryPoolDepletedTimeTotal";
      private static final String MEMORY_POOL_AVG_DEPLETED_PERCENT = "MemoryPoolAvgDepletedPercent";
      private static final String BROKER_CONNECTION_ACCEPT_RATE = "broker-connection-accept-rate";

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
    public record SocketListenerMetric(BeanObject beanObject) implements HasBeanObject {
      private static final String CONNECTION_ACCEPT_THROTTLE_TIME =
          "connection-accept-throttle-time";
      private static final String CONNECTION_ACCEPT_RATE = "connection-accept-rate";
      private static final String IP_CONNECTION_ACCEPT_THROTTLE_TIME =
          "ip-connection-accept-throttle-time";

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
    public record SocketNetworkProcessorMetric(BeanObject beanObject) implements HasBeanObject {
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
    public record Client(BeanObject beanObject) implements HasBeanObject {

      private static final String CONNECTIONS = "connections";

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
