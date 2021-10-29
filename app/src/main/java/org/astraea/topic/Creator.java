package org.astraea.topic;

import java.util.Map;

public interface Creator {
  Creator topic(String topic);

  Creator numberOfPartitions(int numberOfPartitions);

  Creator numberOfReplicas(short numberOfReplicas);

  /**
   * @param configs used to create new topic
   * @return this creator
   */
  Creator configs(Map<String, String> configs);

  /** start to create topic. */
  void create();
}
