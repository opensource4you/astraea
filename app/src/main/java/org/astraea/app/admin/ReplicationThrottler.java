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
   * Maximum bandwidth for follower log broker to accept replicated data.
   *
   * @param limitForEachFollowerBroker the maximum bandwidth to throttle for each broker
   * @return this
   */
  ReplicationThrottler ingress(DataRate limitForEachFollowerBroker);

  /**
   * Maximum bandwidth for follower log broker to accept replicated data.
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
   * Maximum bandwidth for leader log broker to transmit replicated data.
   *
   * @param limitForEachLeaderBroker the maximum bandwidth to throttle for each leader log broker.
   * @return this
   */
  ReplicationThrottler egress(DataRate limitForEachLeaderBroker);

  /**
   * Maximum bandwidth for leader log broker to transmit replicated data.
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
   * Declare that every log under the specified topic, its replication will be throttle.
   *
   * @param topic throttle every log under this topic.
   * @return this
   */
  ReplicationThrottler throttle(String topic);

  /**
   * Declare that every log under the specified topic/partition<strong>(look up at the applying
   * moment)</strong>, its replication will be throttle.
   *
   * @param topicPartition throttle all its logs
   * @return this
   */
  ReplicationThrottler throttle(TopicPartition topicPartition);

  /**
   * Declare that the replication bandwidth for the given log should be throttled, also attempt to
   * resolve the actual leader/follower identity of the log.
   *
   * <p>There are two kind of throttle target config, one for leader({@code
   * leader.replication.throttled.replicas}) and another for follower({@code
   * follower.replication.throttled.replicas}). For this API, only one of the configs will be
   * updated for the given log. The config to update is determined by the identity of the given log
   * at the applying moment. The given log must be part of the replica list at the applying moment.
   * Otherwise, an exception will be raised due to the given log having no leader/follower identity.
   * To throttle a log that are not present at the current cluster, consider use {@link
   * ReplicationThrottler#throttleLeader(TopicPartitionReplica)} or {@link
   * ReplicationThrottler#throttleFollower(TopicPartitionReplica)}.
   *
   * @param replica throttle this log
   * @return this
   */
  ReplicationThrottler throttle(TopicPartitionReplica replica);

  /**
   * Declare that the replication bandwidth for the given leader log should be throttled.
   *
   * @param replica throttle this leader log
   * @return this
   */
  ReplicationThrottler throttleLeader(TopicPartitionReplica replica);

  /**
   * Declare that the replication bandwidth for the given follower log should be throttled.
   *
   * @param replica throttle this follower log
   * @return this
   */
  ReplicationThrottler throttleFollower(TopicPartitionReplica replica);

  /** Apply the throttle setting to the cluster. */
  void apply();
}
