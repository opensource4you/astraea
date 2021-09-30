package org.astraea.metrics.kafka.metrics;

public enum BrokerTopicMetrics {
  /** Message validation failure rate due to non-continuous offset or sequence number in batch */
  InvalidOffsetOrSequenceRecordsPerSec,
  /** Message validation failure rate due to incorrect crc checksum */
  InvalidMessageCrcRecordsPerSec,
  FetchMessageConversionsPerSec,
  BytesRejectedPerSec,
  /** Message in rate */
  MessagesInPerSec,
  /** Incoming byte rate of reassignment traffic */
  ReassignmentBytesInPerSec,
  FailedFetchRequestsPerSec,
  /** Byte in rate from other brokers */
  ReplicationBytesInPerSec,
  /** Message validation failure rate due to no key specified for compacted topic */
  NoKeyCompactedTopicRecordsPerSec,
  TotalFetchRequestsPerSec,
  FailedProduceRequestsPerSec,
  /** Byte in rate from clients */
  BytesInPerSec,
  TotalProduceRequestsPerSec,
  /** Message validation failure rate due to invalid magic number */
  InvalidMagicNumberRecordsPerSec,
  /** Outgoing byte rate of reassignment traffic */
  ReassignmentBytesOutPerSec,
  /** Bytes in rate from other brokers */
  ReplicationBytesOutPerSec,
  ProduceMessageConversionsPerSec,
  /** Byte out rate to clients. */
  BytesOutPerSec;
}
