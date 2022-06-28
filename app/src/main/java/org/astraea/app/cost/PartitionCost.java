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
package org.astraea.app.cost;

import java.util.Map;
import org.astraea.app.admin.TopicPartition;

/** Return type of cost function, `HasPartitionCost`. It returns the score of partitions. */
public interface PartitionCost {
  /**
   * Get the cost of all leader partitions with the given topic name.
   *
   * @param topic the topic name we want to query for.
   * @return the cost of all leader partitions, with respect to the given topic.
   */
  Map<TopicPartition, Double> value(String topic);

  /**
   * Get the cost of all partitions (leader/followers) with the given broker ID.
   *
   * @param brokerId the broker we want to query for.
   * @return the cost of all partitions (leader/followers), with respect to the given broker.
   */
  Map<TopicPartition, Double> value(int brokerId);
}
