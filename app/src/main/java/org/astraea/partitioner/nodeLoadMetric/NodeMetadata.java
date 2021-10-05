package org.astraea.partitioner.nodeLoadMetric;

public class NodeMetadata {
    private String nodeID;
    private NodeMetrics nodeMetrics;
    private double totalBytes;
    private int overLoadCount;

    NodeMetadata (String nodeID, NodeMetrics nodeMetrics) {
        this.nodeID = nodeID;
        this.nodeMetrics = nodeMetrics;
        this.overLoadCount = 0;
        this.totalBytes = 0.0;
    }

    public NodeMetrics getNodeMetrics() {
        return this.nodeMetrics;
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
