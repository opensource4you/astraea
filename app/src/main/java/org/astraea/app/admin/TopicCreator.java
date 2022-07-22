/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.astraea.app.admin;

import java.time.Duration;
import java.util.Map;
import org.apache.kafka.common.config.TopicConfig;

public interface TopicCreator {
  TopicCreator topic(String topic);

  TopicCreator numberOfPartitions(int numberOfPartitions);

  TopicCreator numberOfReplicas(short numberOfReplicas);

  /**
   * set the cleanup policy to compact. By default, the policy is `delete`/
   *
   * @return this creator
   */
  default TopicCreator compacted() {
    return config(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT);
  }

  /**
   * the max time to make segments be eligible for compaction.
   *
   * @param value the max lag
   * @return this creator
   */
  default TopicCreator compactionMaxLag(Duration value) {
    return compacted()
        .config(TopicConfig.MAX_COMPACTION_LAG_MS_CONFIG, String.valueOf(value.toMillis()));
  }

  default TopicCreator compression(Compression compression) {
    return config(TopicConfig.COMPRESSION_TYPE_CONFIG, compression.nameOfKafka());
  }

  TopicCreator config(String key, String value);

  /**
   * @param configs used to create new topic
   * @return this creator
   */
  TopicCreator configs(Map<String, String> configs);

  /** start to create topic. */
  void create();
}
