package org.astraea.partitioner;

import java.util.Map;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

public interface Dispatcher extends Partitioner {
  /**
   * Compute the partition for the given record.
   *
   * @param topic The topic name
   * @param key The key to partition on
   * @param value The value to partition
   * @param clusterInfo The current cluster metadata
   */
  int partition(String topic, byte[] key, byte[] value, ClusterInfo clusterInfo);

  void configure(Configuration config);

  @Override
  default void configure(Map<String, ?> configs) {
    configure(Configuration.of(configs));
  }

  @Override
  default int partition(
      String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
    return partition(
        topic,
        keyBytes == null ? new byte[0] : keyBytes,
        valueBytes == null ? new byte[0] : valueBytes,
        ClusterInfo.of(cluster));
  }

  @Override
  default void onNewBatch(String topic, Cluster cluster, int prevPartition) {}
}
