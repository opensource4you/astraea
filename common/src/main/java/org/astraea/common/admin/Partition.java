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

import java.util.List;
import java.util.Optional;

public interface Partition {
  static Partition of(
      String topic,
      int partition,
      NodeInfo leader,
      List<NodeInfo> replicas,
      List<NodeInfo> isr,
      long earliestOffset,
      long latestOffset,
      Optional<Long> maxTimestamp,
      boolean internal) {
    return new Partition() {

      @Override
      public String topic() {
        return topic;
      }

      @Override
      public int partition() {
        return partition;
      }

      @Override
      public long earliestOffset() {
        return earliestOffset;
      }

      @Override
      public long latestOffset() {
        return latestOffset;
      }

      @Override
      public Optional<Long> maxTimestamp() {
        return maxTimestamp;
      }

      @Override
      public Optional<NodeInfo> leader() {
        return Optional.ofNullable(leader);
      }

      @Override
      public List<NodeInfo> replicas() {
        return replicas;
      }

      @Override
      public List<NodeInfo> isr() {
        return isr;
      }

      @Override
      public boolean internal() {
        return internal;
      }
    };
  }

  default TopicPartition topicPartition() {
    return TopicPartition.of(topic(), partition());
  }

  String topic();

  int partition();

  /** @return existent earliest offset */
  long earliestOffset();

  /** @return existent latest offset */
  long latestOffset();

  /**
   * @return max timestamp of existent records. If the kafka servers don't support to fetch max
   *     timestamp, this method will return empty
   */
  Optional<Long> maxTimestamp();

  /** @return null if the node gets offline. otherwise, it returns node info. */
  Optional<NodeInfo> leader();

  List<NodeInfo> replicas();

  List<NodeInfo> isr();

  /** @return true if this topic is internal (system) topic */
  boolean internal();
}
