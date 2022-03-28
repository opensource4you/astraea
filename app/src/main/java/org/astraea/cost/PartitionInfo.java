package org.astraea.cost;

public interface PartitionInfo {

  static PartitionInfo of(org.apache.kafka.common.PartitionInfo pf) {
    return of(pf.topic(), pf.partition(), NodeInfo.of(pf.leader()));
  }

  static PartitionInfo of(String topic, int partition, NodeInfo leader) {
    return new PartitionInfo() {
      @Override
      public String topic() {
        return topic;
      }

      @Override
      public int partition() {
        return partition;
      }

      @Override
      public NodeInfo leader() {
        return leader;
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
