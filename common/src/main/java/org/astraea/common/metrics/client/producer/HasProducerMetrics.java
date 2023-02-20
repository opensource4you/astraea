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
package org.astraea.common.metrics.client.producer;

import org.astraea.common.metrics.client.HasSelectorMetrics;

@FunctionalInterface
public interface HasProducerMetrics extends HasSelectorMetrics {

  default double batchSizeAvg() {
    return (double) beanObject().attributes().get("batch-size-avg");
  }

  default double batchSizeMax() {
    return (double) beanObject().attributes().get("batch-size-max");
  }

  default double batchSplitRate() {
    return (double) beanObject().attributes().get("batch-split-rate");
  }

  default double batchSplitTotal() {
    return (double) beanObject().attributes().get("batch-split-total");
  }

  default double bufferAvailableBytes() {
    return (double) beanObject().attributes().get("buffer-available-bytes");
  }

  default double bufferExhaustedRate() {
    return (double) beanObject().attributes().get("buffer-exhausted-rate");
  }

  default double bufferExhaustedTotal() {
    return (double) beanObject().attributes().get("buffer-exhausted-total");
  }

  default double bufferTotalBytes() {
    return (double) beanObject().attributes().get("buffer-total-bytes");
  }

  default double bufferPoolWaitRatio() {
    return (double) beanObject().attributes().get("bufferpool-wait-ratio");
  }

  default double bufferPoolWaitTimeNsTotal() {
    return (double) beanObject().attributes().get("bufferpool-wait-time-ns-total");
  }

  default double compressionRateAvg() {
    return (double) beanObject().attributes().get("compression-rate-avg");
  }

  default double flushTimeNsTotal() {
    return (double) beanObject().attributes().get("flush-time-ns-total");
  }

  default double metadataAge() {
    return (double) beanObject().attributes().get("metadata-age");
  }

  default double produceThrottleTimeAvg() {
    return (double) beanObject().attributes().get("produce-throttle-time-avg");
  }

  default double produceThrottleTimeMax() {
    return (double) beanObject().attributes().get("produce-throttle-time-max");
  }

  default double recordErrorRate() {
    return (double) beanObject().attributes().get("record-error-rate");
  }

  default double recordErrorTotal() {
    return (double) beanObject().attributes().get("record-error-total");
  }

  default double recordQueueTimeAvg() {
    return (double) beanObject().attributes().get("record-queue-time-avg");
  }

  default double recordQueueTimeMax() {
    return (double) beanObject().attributes().get("record-queue-time-max");
  }

  default double recordRetryRate() {
    return (double) beanObject().attributes().get("record-retry-rate");
  }

  default double recordRetryTotal() {
    return (double) beanObject().attributes().get("record-retry-total");
  }

  default double recordSendRate() {
    return (double) beanObject().attributes().get("record-send-rate");
  }

  default double recordSendTotal() {
    return (double) beanObject().attributes().get("record-send-total");
  }

  default double recordSizeAvg() {
    return (double) beanObject().attributes().get("record-size-avg");
  }

  default double recordSizeMax() {
    return (double) beanObject().attributes().get("record-size-max");
  }

  default double recordsPerRequestAvg() {
    return (double) beanObject().attributes().get("records-per-request-avg");
  }

  default double requestLatencyAvg() {
    return (double) beanObject().attributes().get("request-latency-avg");
  }

  default double requestLatencyMax() {
    return (double) beanObject().attributes().get("request-latency-max");
  }

  default double requestInFlight() {
    return (double) beanObject().attributes().get("requests-in-flight");
  }

  default double txnAbortTimeNsTotal() {
    return (double) beanObject().attributes().get("txn-abort-time-ns-total");
  }

  default double txnBeginTimeNsTotal() {
    return (double) beanObject().attributes().get("txn-begin-time-ns-total");
  }

  default double txnCommitTimeNsTotal() {
    return (double) beanObject().attributes().get("txn-commit-time-ns-total");
  }

  default double txnInitTimeNsTotal() {
    return (double) beanObject().attributes().get("txn-init-time-ns-total");
  }

  default double txnSendOffsetsTimeNsTotal() {
    return (double) beanObject().attributes().get("txn-send-offsets-time-ns-total");
  }

  default double waitingThreads() {
    return (double) beanObject().attributes().get("waiting-threads");
  }
}
