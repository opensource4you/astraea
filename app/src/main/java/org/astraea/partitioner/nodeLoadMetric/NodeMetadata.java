package org.astraea.partitioner.nodeLoadMetric;

/** Store information about each node */
public class NodeMetadata implements SafeMetadata {
  private final String nodeID;
  private double totalBytes;
  private int overLoadCount;

  NodeMetadata(String nodeID) {
    this.nodeID = nodeID;
    this.overLoadCount = 0;
    this.totalBytes = 0.0;
  }

  public void setOverLoadCount(int count) {
    this.overLoadCount = count;
  }

  public void setTotalBytes(double bytes) {
    this.totalBytes = bytes;
  }

  public double getTotalBytes() {
    return this.totalBytes;
  }

  public String getNodeID() {
    return this.nodeID;
  }

  public int getOverLoadCount() {
    return this.overLoadCount;
  }
}
