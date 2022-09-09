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
import org.astraea.common.DataRate;

/**
 * Offer a friendly interface to throttle replication bandwidth. See the proposal and discussion at
 * <a href="https://cwiki.apache.org/confluence/display/KAFKA/KIP-73+Replication+Quotas">KIP-73</a>
 * for further detail.
 */
public interface ReplicationThrottler {

  /**
   * Maximum bandwidth for follower log broker to accept replicated data.
   *
   * <p>This setting will apply to every live broker in the cluster, since it is targeting the live
   * one, any offline broker will not be covered.
   *
   * @param limitForEachFollowerBroker the maximum replication traffic-in of broker.
   * @return this
   */
  ReplicationThrottler ingress(DataRate limitForEachFollowerBroker);

  /**
   * Maximum bandwidth for follower log broker to accept replicated data.
   *
   * @param limitPerFollowerBroker the maximum replication traffic-in for every specified broker.
   * @return this
   */
  ReplicationThrottler ingress(Map<Integer, DataRate> limitPerFollowerBroker);

  /**
   * Maximum bandwidth for leader log broker to transmit replicated data.
   *
   * <p>This setting will apply to every live broker in the cluster, since it is targeting the live
   * one, any offline broker will not be covered.
   *
   * @param limitForEachLeaderBroker the maximum replication traffic-out of broker.
   * @return this
   */
  ReplicationThrottler egress(DataRate limitForEachLeaderBroker);

  /**
   * Maximum bandwidth for leader log broker to transmit replicated data.
   *
   * @param limitPerLeaderBroker the maximum replication traffic-out for every specified broker.
   * @return this
   */
  ReplicationThrottler egress(Map<Integer, DataRate> limitPerLeaderBroker);

  /**
   * Declare that every log currently under the specified topic, its replication will be throttle.
   *
   * <p>This API doesn't throttle any partitions or replicas that create/alter in the future. It
   * only applies to the log currently under that topic.
   *
   * <p>This API can't be used in conjunction with the wildcard throttle. An attempt to do so will
   * result in an exception.
   *
   * @param topic throttle every log under this topic.
   * @return this
   */
  ReplicationThrottler throttle(String topic);

  /**
   * Declare that the current logs under the specified topic/partition<strong>(look up at the
   * calling moment)</strong>, its replication will be throttle. This lookup occurred at the calling
   * moment of this function , so any replica change that happened in the future might not be
   * included in this setting.
   *
   * <p>This API can't be used in conjunction with the wildcard throttle. An attempt to do so will
   * result in an exception.
   *
   * @param topicPartition throttle the logs belong to this topic/partition(seek at the calling
   *     moment).
   * @return this
   */
  ReplicationThrottler throttle(TopicPartition topicPartition);

  /**
   * Declare that the replication bandwidth for the given log should be throttled.
   *
   * <p>This API can't be used in conjunction with the wildcard throttle. An attempt to do so will
   * result in an exception.
   *
   * @param replica throttle this log.
   * @return this
   */
  ReplicationThrottler throttle(TopicPartitionReplica replica);

  /**
   * Declare that the replication bandwidth for the given leader log should be throttled.
   *
   * @param replica throttle this leader log.
   * @return this
   */
  ReplicationThrottler throttleLeader(TopicPartitionReplica replica);

  /**
   * Declare that the replication bandwidth for the given follower log should be throttled.
   *
   * @param replica throttle this follower log.
   * @return this
   */
  ReplicationThrottler throttleFollower(TopicPartitionReplica replica);

  /**
   * Apply the throttle setting to the cluster.
   *
   * @return an {@link AffectedResources} object that describe the resource that will be affected
   *     after applied this throttle.
   */
  AffectedResources apply();

  interface AffectedResources {
    Map<Integer, DataRate> ingress();

    Map<Integer, DataRate> egress();

    Set<TopicPartitionReplica> leaders();

    Set<TopicPartitionReplica> followers();
  }
}
