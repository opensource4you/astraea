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
package org.astraea.app.metrics.client.consumer;

import org.astraea.app.metrics.HasBeanObject;

public interface HasConsumerCoordinatorMetrics extends HasBeanObject {

  default String clientId() {
    return beanObject().properties().get("client-id");
  }

  default double assignedPartitions() {
    return (double) beanObject().attributes().get("assigned-partitions");
  }

  default double commitLatencyAvg() {
    return (double) beanObject().attributes().get("commit-latency-avg");
  }

  default double commitLatencyMax() {
    return (double) beanObject().attributes().get("commit-latency-max");
  }

  default double commitRate() {
    return (double) beanObject().attributes().get("commit-rate");
  }

  default double commitTotal() {
    return (double) beanObject().attributes().get("commit-total");
  }

  default double failedRebalanceRatePerHour() {
    return (double) beanObject().attributes().get("failed-rebalance-rate-per-hour");
  }

  default double failedRebalanceTotal() {
    return (double) beanObject().attributes().get("failed-rebalance-total");
  }

  default double heartbeatRate() {
    return (double) beanObject().attributes().get("heartbeat-rate");
  }

  default double joinRate() {
    return (double) beanObject().attributes().get("join-rate");
  }

  default double joinTimeAvg() {
    return (double) beanObject().attributes().get("join-time-avg");
  }

  default double joinTimeMax() {
    return (double) beanObject().attributes().get("join-time-max");
  }

  default double joinTotal() {
    return (double) beanObject().attributes().get("join-total");
  }

  default double lastHeartbeatSecondsAgo() {
    return (double) beanObject().attributes().get("last-heartbeat-seconds-ago");
  }

  default double lastRebalanceSecondsAgo() {
    return (double) beanObject().attributes().get("last-rebalance-seconds-ago");
  }

  default double partitionAssignedLatencyAvg() {
    return (double) beanObject().attributes().get("partition-assigned-latency-avg");
  }

  default double partitionAssignedLatencyMax() {
    return (double) beanObject().attributes().get("partition-assigned-latency-max");
  }

  default double partitionLostLatencyAvg() {
    return (double) beanObject().attributes().get("partition-lost-latency-avg");
  }

  default double partitionLostLatencyMax() {
    return (double) beanObject().attributes().get("partition-lost-latency-max");
  }

  default double partitionRevokedLatencyAvg() {
    return (double) beanObject().attributes().get("partition-revoked-latency-avg");
  }

  default double partitionRevokedLatencyMax() {
    return (double) beanObject().attributes().get("partition-revoked-latency-max");
  }

  default double rebalanceLatencyAvg() {
    return (double) beanObject().attributes().get("rebalance-latency-avg");
  }

  default double rebalanceLatencyMax() {
    return (double) beanObject().attributes().get("rebalance-latency-max");
  }

  default double rebalanceLatencyTotal() {
    return (double) beanObject().attributes().get("rebalance-latency-total");
  }

  default double rebalanceRatePerHour() {
    return (double) beanObject().attributes().get("rebalance-rate-per-hour");
  }

  default double rebalanceTotal() {
    return (double) beanObject().attributes().get("rebalance-total");
  }

  default double syncRate() {
    return (double) beanObject().attributes().get("sync-rate");
  }

  default double syncTimeAvg() {
    return (double) beanObject().attributes().get("sync-time-avg");
  }

  default double syncTimeMax() {
    return (double) beanObject().attributes().get("sync-time-max");
  }

  default double syncTotal() {
    return (double) beanObject().attributes().get("sync-total");
  }
}
