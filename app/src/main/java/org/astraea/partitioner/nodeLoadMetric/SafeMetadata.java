package org.astraea.partitioner.nodeLoadMetric;

public interface SafeMetadata {
  /** @return The nodeID. */
  public String getNodeID();
  /** @return The Sum of node InputPerSec and OutputPerSec. */
  public double getTotalBytes();

  /** @return The count of node overload in ten seconds.（Binary conversion required） */
  public int getOverLoadCount();
}
