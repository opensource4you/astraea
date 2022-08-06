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

import java.util.Map;
import org.astraea.app.common.DataRate;

/**
 * Offer a friendly interface to throttle replication bandwidth. See the proposal and discussion at
 * <a href="https://cwiki.apache.org/confluence/display/KAFKA/KIP-73+Replication+Quotas">KIP-73</a>
 * for further detail.
 */
public interface ReplicationThrottler {

  /**
   * Bandwidth for follower log broker to replicate data from the leader log broker.
   *
   * @param limitForEachFollowerBroker the maximum bandwidth to throttle for each broker
   * @return this
   */
  ReplicationThrottler ingress(DataRate limitForEachFollowerBroker);

  /**
   * Bandwidth for follower log broker to replicate data from the leader log broker.
   *
   * <p>For any follower broker that the throttle bandwidth is not specified in the argument Map.
   * The value from {@link ReplicationThrottler#ingress(DataRate)} will be used. If that value is
   * not specified either, an exception will be raised.
   *
   * @param limitPerFollowerBroker the maximum bandwidth to throttle for every specified broker
   * @return this
   */
  ReplicationThrottler ingress(Map<Integer, DataRate> limitPerFollowerBroker);

  /**
   * Bandwidth for leader log broker to replicate data to all the follower log brokers.
   *
   * @param limitForEachLeaderBroker the maximum bandwidth to throttle for each leader log broker.
   * @return this
   */
  ReplicationThrottler egress(DataRate limitForEachLeaderBroker);

  /**
   * Bandwidth for leader log broker to replicate data to all the follower log brokers.
   *
   * <p>For any leader broker that the throttle bandwidth is not specified in the argument Map. The
   * value from {@link ReplicationThrottler#egress(DataRate)} will be used. If that value is not
   * specified either, an exception will be raised.
   *
   * @param limitPerLeaderBroker the maximum bandwidth to throttle for every specified broker.
   * @return this
   */
  ReplicationThrottler egress(Map<Integer, DataRate> limitPerLeaderBroker);

  /**
   * Every log under the specified topic, its replication will be throttle.
   *
   * @param topic throttle every log under this topic.
   * @return this
   */
  ReplicationThrottler throttleTopic(String topic);

  /**
   * Every logs under the specified topic/partition<strong>(look up at the applying
   * moment)</strong>, its replication will be throttle.
   *
   * @param topicPartition throttle all its logs
   * @return this
   */
  ReplicationThrottler throttleLogs(TopicPartition topicPartition);

  /**
   * The leader log and any non-synced logs under the specified topic/partition<strong>(look up at
   * the applying moment)</strong>, its replication will be throttle.
   *
   * <p>If every log are synced, no throttle will be applied.
   *
   * @param topicPartition throttle its non-synced logs
   * @return this
   */
  ReplicationThrottler throttleNonSyncedLogs(TopicPartition topicPartition);

  /**
   * Throttle the replication for the given log.
   *
   * @param replica throttle this log
   * @return this
   */
  ReplicationThrottler throttleLog(TopicPartitionReplica replica);

  /** Apply throttle setting to the cluster. */
  void apply();
}
