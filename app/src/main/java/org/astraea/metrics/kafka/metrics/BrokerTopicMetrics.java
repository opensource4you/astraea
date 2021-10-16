package org.astraea.metrics.kafka.metrics;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.astraea.metrics.jmx.BeanObject;
import org.astraea.metrics.jmx.BeanQuery;

public enum BrokerTopicMetrics implements Metric<BrokerTopicMetricsResult> {
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
  public static final Set<String> AllMetricNames =
      Arrays.stream(values())
          .map(BrokerTopicMetrics::metricName)
          .collect(Collectors.toUnmodifiableSet());

  BrokerTopicMetrics(String name) {
    this.metricName = name;
  }

  public String metricName() {
    return metricName;
  }

  @Override
  public List<BeanQuery> queries() {
    return List.of(
        BeanQuery.builder("kafka.server")
            .property("type", "BrokerTopicMetrics")
            .property("name", this.metricName())
            .build());
  }

  @Override
  public BrokerTopicMetricsResult from(List<BeanObject> beanObject) {
    return new BrokerTopicMetricsResult(this, beanObject.get(0));
  }
}
