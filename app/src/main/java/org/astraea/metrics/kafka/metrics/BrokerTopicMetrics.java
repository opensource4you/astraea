package org.astraea.metrics.kafka.metrics;

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
}
