package org.astraea.partitioner;

import java.util.Map;
import java.util.stream.Collectors;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.astraea.cost.ClusterInfo;

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

  /**
   * configure this dispatcher. This method is called only once.
   *
   * @param config configuration
   */
  default void configure(Configuration config) {}

  /** close this dispatcher. This method is executed only once. */
  @Override
  default void close() {}

  @Override
  default void configure(Map<String, ?> configs) {
    configure(
        Configuration.of(
            configs.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().toString()))));
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
