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
package org.astraea.common.consumer;

import org.astraea.common.admin.TopicPartition;

import java.time.Duration;
import java.util.Optional;
import java.util.Set;

/**
 * This inherited consumer offers function related to consumer group.
 *
 * @param <Key> key
 * @param <Value> value
 */
public interface SubscribedConsumer<Key, Value> extends Consumer<Key, Value> {

  /**
   * commit the consumed offsets right now.
   *
   * @param timeout to wait commit.
   */
  void commitOffsets(Duration timeout);

  /** @return the group id including this consumer */
  String groupId();

  /** @return the member id used by this consumer */
  String memberId();

  /** @return group instance id (static member) */
  Optional<String> groupInstanceId();

  Set<TopicPartition> stickyPartitions();

  boolean checkRebalance();
}
