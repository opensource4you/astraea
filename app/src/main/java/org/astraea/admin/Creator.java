package org.astraea.admin;

import java.time.Duration;
import java.util.Map;
import org.apache.kafka.common.config.TopicConfig;

public interface Creator {
  Creator topic(String topic);

  Creator numberOfPartitions(int numberOfPartitions);

  Creator numberOfReplicas(short numberOfReplicas);

  /**
   * set the cleanup policy to compact. By default, the policy is `delete`/
   *
   * @return this creator
   */
  default Creator compacted() {
    return config(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT);
  }

  /**
   * the max time to make segments be eligible for compaction.
   *
   * @param value the max lag
   * @return this creator
   */
  default Creator compactionMaxLag(Duration value) {
    return compacted()
        .config(TopicConfig.MAX_COMPACTION_LAG_MS_CONFIG, String.valueOf(value.toMillis()));
  }

  default Creator compression(Compression compression) {
    return config(TopicConfig.COMPRESSION_TYPE_CONFIG, compression.nameOfKafka());
  }

  Creator config(String key, String value);

  /**
   * @param configs used to create new topic
   * @return this creator
   */
  Creator configs(Map<String, String> configs);

  /** start to create topic. */
  void create();
}
