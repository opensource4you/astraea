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
package org.astraea.common.cost;

import java.util.Map;
import java.util.Set;
import org.astraea.common.admin.TopicPartition;

/**
 * Return the type of the cost function, `HasPartitionCost`. It returns the score of the
 * topic-partitions.
 */
@FunctionalInterface
public interface PartitionCost {

  /**
   * @return Topic-partition and its cost.
   */
  Map<TopicPartition, Double> value();

  /**
   * Because assigning some partitions to the same consumer has an effect on throughput and latency
   * (such as when assigning partitions with significantly different traffic to the same consumer).
   *
   * <p>This method provides the feedback is used to determine which partitions should not be
   * assigned together to avoid the effect of the consumer's throughput and latency.
   *
   * @return The feedback on determining which partitions cannot be put together.
   */
  default Map<TopicPartition, Set<TopicPartition>> incompatibility() {
    return Map.of();
  }
}
