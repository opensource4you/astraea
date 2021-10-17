package org.astraea.metrics.kafka;

import org.astraea.metrics.jmx.BeanObject;
import org.astraea.metrics.jmx.BeanQuery;
import org.astraea.metrics.jmx.MBeanClient;
import org.astraea.metrics.kafka.metrics.BrokerTopicMetricsResult;
import org.astraea.metrics.kafka.metrics.TotalTimeMs;

public final class KafkaMetrics {

  private KafkaMetrics() {}

  public enum BrokerTopicMetrics {
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

    BrokerTopicMetrics(String name) {
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
      return new BrokerTopicMetricsResult(this, beanObject);
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

  public enum RequestMetrics {
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
  }
}
