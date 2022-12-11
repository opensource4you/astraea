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
package org.astraea.common.metrics.connector;

import org.astraea.common.metrics.HasBeanObject;

@FunctionalInterface
public interface SourceTaskInfo extends HasBeanObject {

  default String connectorName() {
    return beanObject().properties().get("connector");
  }

  default int taskId() {
    return Integer.parseInt(beanObject().properties().get("task"));
  }

  default double pollBatchAvgTimeMs() {
    return (double) beanObject().attributes().get("poll-batch-avg-time-ms");
  }

  default double pollBatchMaxTimeMs() {
    return (double) beanObject().attributes().get("poll-batch-max-time-ms");
  }

  default double sourceRecordActiveCount() {
    return (double) beanObject().attributes().get("source-record-active-count");
  }

  default double sourceRecordActiveCountAvg() {
    return (double) beanObject().attributes().get("source-record-active-count-avg");
  }

  default double sourceRecordActiveCountMax() {
    return (double) beanObject().attributes().get("source-record-active-count-max");
  }

  default double sourceRecordPollRate() {
    return (double) beanObject().attributes().get("source-record-poll-rate");
  }

  default double sourceRecordPollTotal() {
    return (double) beanObject().attributes().get("source-record-poll-total");
  }

  default double sourceRecordWriteRate() {
    return (double) beanObject().attributes().get("source-record-write-rate");
  }

  default double sourceRecordWriteTotal() {
    return (double) beanObject().attributes().get("source-record-write-total");
  }
}
