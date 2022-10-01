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

import org.astraea.common.metrics.HasBeanObject;

public interface HasConsumerFetchMetrics extends HasBeanObject {

  default String clientId() {
    return beanObject().properties().get("client-id");
  }

  default double bytesConsumedRate() {
    return (double) beanObject().attributes().get("bytes-consumed-rate");
  }

  default double bytesConsumedTotal() {
    return (double) beanObject().attributes().get("bytes-consumed-total");
  }

  default double fetchLatencyAvg() {
    return (double) beanObject().attributes().get("fetch-latency-avg");
  }

  default double fetchLatencyMax() {
    return (double) beanObject().attributes().get("fetch-latency-max");
  }

  default double fetchRate() {
    return (double) beanObject().attributes().get("fetch-rate");
  }

  default double fetchSizeAvg() {
    return (double) beanObject().attributes().get("fetch-size-avg");
  }

  default double fetchSizeMax() {
    return (double) beanObject().attributes().get("fetch-size-max");
  }

  default double fetchThrottleTimeAvg() {
    return (double) beanObject().attributes().get("fetch-throttle-time-avg");
  }

  default double fetchThrottleTimeMax() {
    return (double) beanObject().attributes().get("fetch-throttle-time-max");
  }

  default double fetchTotal() {
    return (double) beanObject().attributes().get("fetch-total");
  }

  default double recordsConsumedRate() {
    return (double) beanObject().attributes().get("records-consumed-rate");
  }

  default double recordsConsumedTotal() {
    return (double) beanObject().attributes().get("records-consumed-total");
  }

  default double recordsLagMax() {
    return (double) beanObject().attributes().get("records-lag-max");
  }

  default double recordsLeadMin() {
    return (double) beanObject().attributes().get("records-lead-min");
  }

  default double recordsPerRequestAvg() {
    return (double) beanObject().attributes().get("records-per-request-avg");
  }
}
