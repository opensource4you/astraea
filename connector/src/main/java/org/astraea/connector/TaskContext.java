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
package org.astraea.connector;

import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.astraea.common.admin.TopicPartition;

public interface TaskContext {

  /**
   * Reset the consumer offsets for the specified partitions.
   *
   * @param offsets The map of offsets to commit. The key is the {@link TopicPartition} and the
   *     value is the offset to reset to.
   */
  void offset(Map<TopicPartition, Long> offsets);

  /**
   * Reset the consumer offset for the specified partition.
   *
   * @param topicPartition The {@link TopicPartition} to reset.
   * @param offset The offset to reset to.
   */
  void offset(TopicPartition topicPartition, long offset);

  /**
   * Pause the specified partitions for consuming messages.
   *
   * @param partitions The collection of partitions should be paused.
   */
  void pause(Collection<TopicPartition> partitions);

  /**
   * Request an offset commit. This is asynchronous and may not be complete when the method returns.
   */
  void requestCommit();

  static TaskContext of(SinkTaskContext context) {
    return new TaskContext() {
      @Override
      public void offset(Map<TopicPartition, Long> offsets) {
        context.offset(
            offsets.entrySet().stream()
                .collect(
                    Collectors.toMap(e -> TopicPartition.to(e.getKey()), Map.Entry::getValue)));
      }

      @Override
      public void offset(TopicPartition topicPartition, long offset) {
        context.offset(TopicPartition.to(topicPartition), offset);
      }

      @Override
      public void pause(Collection<TopicPartition> partitions) {
        context.pause(
            partitions.stream()
                .map(TopicPartition::to)
                .toArray(org.apache.kafka.common.TopicPartition[]::new));
      }

      @Override
      public void requestCommit() {
        context.requestCommit();
      }
    };
  }
}
