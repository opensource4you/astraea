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
   * move partition to specify folder.
   *
   * @param brokerFolders contain
   */
  void moveTo(Map<Integer, String> brokerFolders);
}
