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
package org.astraea.common.metrics.client.consumer;

import org.astraea.common.metrics.client.HasCoordinatorMetrics;

@FunctionalInterface
public interface HasConsumerCoordinatorMetrics extends HasCoordinatorMetrics {

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
}
