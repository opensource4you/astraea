package org.astraea.metrics.kafka;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;
import org.astraea.metrics.jmx.BeanObject;
import org.astraea.metrics.jmx.BeanQuery;
import org.astraea.metrics.jmx.MBeanClient;
import org.astraea.metrics.kafka.metrics.BrokerTopicMetricsResult;
import org.astraea.metrics.kafka.metrics.OperatingSystemInfo;
import org.astraea.metrics.kafka.metrics.TotalTimeMs;

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
      BeanObject beanObject =
          mBeanClient
              .tryQueryBean(
                  BeanQuery.builder("kafka.server")
                      .property("type", "BrokerTopicMetrics")
                      .property("name", this.metricName())
                      .build())
              .orElseThrow();
      return new BrokerTopicMetricsResult(beanObject);
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
  }

  public enum Purgatory {
    AlterAcls,
    DeleteRecords,
    ElectLeader,
    Fetch,
    Heartbeat,
    Produce,
    Rebalance;

    public int size(MBeanClient mBeanClient) {
      return (int)
          mBeanClient
              .tryQueryBean(
                  BeanQuery.builder("kafka.server")
                      .property("type", "DelayedOperationPurgatory")
                      .property("delayedOperation", this.name())
                      .property("name", "PurgatorySize")
                      .build())
              .orElseThrow()
              .getAttributes()
              .get("Value");
    }
  }

  public enum Request {
    Produce,
    FetchConsumer,
    FetchFollower;

    public TotalTimeMs totalTimeMs(MBeanClient mBeanClient) {
      BeanObject beanObject =
          mBeanClient
              .tryQueryBean(
                  BeanQuery.builder("kafka.network")
                      .property("type", "RequestMetrics")
                      .property("request", this.name())
                      .property("name", "TotalTimeMs")
                      .build())
              .orElseThrow();
      return new TotalTimeMs(beanObject);
    }
  }

  public static final class TopicPartition {

    private TopicPartition() {}

    /**
     * Number of partitions across all topics in the cluster.
     *
     * @return number of partitions across all topics in the cluster.
     */
    public static int globalPartitionCount(MBeanClient mBeanClient) {
      return (int)
          mBeanClient
              .tryQueryBean(
                  BeanQuery.builder("kafka.controller")
                      .property("type", "KafkaController")
                      .property("name", "GlobalPartitionCount")
                      .build())
              .orElseThrow()
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
              .tryQueryBean(
                  BeanQuery.builder("kafka.server")
                      .property("type", "ReplicaManager")
                      .property("name", "UnderReplicatedPartitions")
                      .build())
              .orElseThrow()
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
      Collection<BeanObject> beanObjects =
          client.queryBeans(
              BeanQuery.builder("kafka.log")
                  .property("type", "Log")
                  .property("topic", topicName)
                  .property("partition", "*")
                  .property("name", "Size")
                  .build());

      // collect result as a map.
      return beanObjects.stream()
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
      BeanObject beanObject =
          mBeanClient
              .tryQueryBean(
                  BeanQuery.builder("java.lang").property("type", "OperatingSystem").build())
              .orElseThrow();
      return new OperatingSystemInfo(beanObject);
    }
  }
}
