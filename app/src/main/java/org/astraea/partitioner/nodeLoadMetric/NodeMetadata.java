package org.astraea.partitioner.nodeLoadMetric;

interface NodeMetadata {
  /** @return The nodeID. */
  String nodeID();

  /** @return The Sum of node InputPerSec and OutputPerSec. */
  double totalBytes();

  /** @return The count of node overload in ten seconds.（Binary conversion required） */
  int overLoadCount();
}
