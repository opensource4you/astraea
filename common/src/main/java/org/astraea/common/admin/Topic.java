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
package org.astraea.common.admin;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.common.TopicPartitionInfo;

/**
 * @param name topic name
 * @param config topic configs. it includes both default configs and override configs
 * @param internal true if this topic is internal (system) topic
 * @param partitionIds partition id related to this topic
 */
public record Topic(String name, Config config, boolean internal, Set<Integer> partitionIds) {

  static Topic of(
      String name,
      org.apache.kafka.clients.admin.TopicDescription topicDescription,
      Map<String, String> kafkaConfig) {
    return new Topic(
        name,
        new Config(kafkaConfig),
        topicDescription.isInternal(),
        topicDescription.partitions().stream()
            .map(TopicPartitionInfo::partition)
            .collect(Collectors.toUnmodifiableSet()));
  }

  public Set<TopicPartition> topicPartitions() {
    return partitionIds.stream()
        .map(id -> new TopicPartition(name, id))
        .collect(Collectors.toUnmodifiableSet());
  }
}
