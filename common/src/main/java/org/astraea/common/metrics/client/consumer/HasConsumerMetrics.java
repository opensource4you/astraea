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

import org.astraea.common.metrics.client.HasSelectorMetrics;

@FunctionalInterface
public interface HasConsumerMetrics extends HasSelectorMetrics {

  default double committedTimeNsTotal() {
    return (double) beanObject().attributes().get("committed-time-ns-total");
  }

  default double commitSyncTimeNsTotal() {
    return (double) beanObject().attributes().get("commit-sync-time-ns-total");
  }

  default double timeBetweenPollAvg() {
    return (double) beanObject().attributes().get("time-between-poll-avg");
  }

  default double lastPollSecondsAgo() {
    return (double) beanObject().attributes().get("last-poll-seconds-ago");
  }

  default double timeBetweenPollMax() {
    return (double) beanObject().attributes().get("time-between-poll-max");
  }

  default double pollIdleRatioAvg() {
    return (double) beanObject().attributes().get("poll-idle-ratio-avg");
  }
}
