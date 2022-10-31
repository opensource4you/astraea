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

public interface Topic {

  static Topic of(
      String name,
      org.apache.kafka.clients.admin.TopicDescription topicDescription,
      Map<String, String> kafkaConfig) {

    var config = Config.of(kafkaConfig);
    var topicPartitions =
        topicDescription.partitions().stream()
            .map(p -> TopicPartition.of(name, p.partition()))
            .collect(Collectors.toUnmodifiableSet());
    return new Topic() {
      @Override
      public String name() {
        return name;
      }

      @Override
      public Config config() {
        return config;
      }

      @Override
      public boolean internal() {
        return topicDescription.isInternal();
      }

      @Override
      public Set<TopicPartition> topicPartitions() {
        return topicPartitions;
      }
    };
  }

  /**
   * @return topic name
   */
  String name();

  /**
   * @return config used by this topic
   */
  Config config();

  /**
   * @return true if this topic is internal (system) topic
   */
  boolean internal();

  Set<TopicPartition> topicPartitions();
}
