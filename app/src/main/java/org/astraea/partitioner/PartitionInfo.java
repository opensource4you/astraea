package org.astraea.partitioner;

public interface PartitionInfo {

  static PartitionInfo of(org.apache.kafka.common.PartitionInfo pf) {
    return new PartitionInfo() {
      @Override
      public String topic() {
        return pf.topic();
      }

      @Override
      public int partition() {
        return pf.partition();
      }

      @Override
      public NodeInfo leader() {
        return NodeInfo.of(pf.leader());
      }
    };
  }

  /** @return topic name */
  String topic();

  /** @return partition id */
  int partition();

  /** @return information of leader node */
  NodeInfo leader();
}
