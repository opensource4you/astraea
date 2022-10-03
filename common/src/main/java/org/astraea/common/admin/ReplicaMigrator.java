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
import java.util.Map;

/** used to migrate partitions to another broker or broker folder. */
public interface ReplicaMigrator {
  /**
   * move all partitions (leader replica and follower replicas) of topic
   *
   * @param topic topic name
   * @return this migrator
   */
  ReplicaMigrator topic(String topic);

  /**
   * move one partition (leader replica and follower replicas) of topic
   *
   * @param topic topic name
   * @param partition partition id
   * @return this migrator
   */
  ReplicaMigrator partition(String topic, int partition);

  /**
   * move all partitions (leader replica and follower replicas) of broker
   *
   * @param broker broker id
   * @return this migrator
   */
  ReplicaMigrator broker(int broker);

  /**
   * move all partitions (leader replica and follower replicas) of topic of broker
   *
   * @param broker broker id
   * @param topic topic name
   * @return this migrator
   */
  ReplicaMigrator topicOfBroker(int broker, String topic);

  /**
   * change the partition replica list. If the current partition leader is kicked out of the
   * partition replica list. A preferred leader election will occur implicitly. The preferred
   * leader(the first replica in the list) will become the new leader of this topic/partition. If
   * one wants the preferred leader election to occur explicitly. Consider using {@link
   * Admin#preferredLeaderElection(TopicPartition)}.
   *
   * @param brokers to host partitions
   */
  void moveTo(List<Integer> brokers);

  /**
   * move the replica to specified data directory. All the specified brokers must be part of the
   * partition's current replica list. Otherwise, an {@link IllegalStateException} will be raised.
   * Noted that this method performs some validation to ensure the declared movement are correct.
   * There is a small chance to declare preferred data directory instead of moving it, due to race
   * condition on the stale data.
   *
   * @param brokerFolders the map contains the declared movement destination. All the specified
   *     brokers must be part of the partition's current replica list. Otherwise, an {@link
   *     IllegalStateException} exception will be raised.
   */
  void moveTo(Map<Integer, String> brokerFolders);

  /**
   * declare the preferred data directories for the specified topic/partition. This method can only
   * declare preferred data directory for the broker that doesn't host a replica for the specified
   * partition. To move specific replica from one data directory to another on the same broker,
   * consider use {@link ReplicaMigrator#moveTo(Map)}. Noted that this method performs some
   * validation to ensure user declaring preferred data directory. There is a small chance to
   * accidentally perform a data directory movement due to stale data.
   *
   * @param preferredDirMap the preferred directory map. With each entry indicate the preferred data
   *     directory(value) for a broker(key). All the specified brokers must not be part of the
   *     partition's current replica list. Otherwise, an {@link IllegalStateException} exception
   *     will be raised.
   */
  void declarePreferredDir(Map<Integer, String> preferredDirMap);
}
