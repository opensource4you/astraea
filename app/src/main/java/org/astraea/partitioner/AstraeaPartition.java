package org.astraea.partitioner;

import org.apache.kafka.common.Cluster;

public interface AstraeaPartition {
  int loadPartition(
      String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster);
}
