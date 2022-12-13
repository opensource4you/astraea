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
public interface SinkTaskInfo extends HasBeanObject {

  default String connectorName() {
    return beanObject().properties().get("connector");
  }

  default int taskId() {
    return Integer.parseInt(beanObject().properties().get("task"));
  }

  default String taskType() {
    return beanObject().properties().get("type");
  }

  default double offsetCommitCompletionRate() {
    return (double) beanObject().attributes().get("offset-commit-completion-rate");
  }

  default double offsetCommitCompletionTotal() {
    return (double) beanObject().attributes().get("offset-commit-completion-total");
  }

  default double offsetCommitSeqNo() {
    return (double) beanObject().attributes().get("offset-commit-seq-no");
  }

  default double offsetCommitSkipRate() {
    return (double) beanObject().attributes().get("offset-commit-skip-rate");
  }

  default double offsetCommitSkipTotal() {
    return (double) beanObject().attributes().get("offset-commit-skip-total");
  }

  default double partitionCount() {
    return (double) beanObject().attributes().get("partition-count");
  }

  default double putBatchAvgTimeMs() {
    return (double) beanObject().attributes().get("put-batch-avg-time-ms");
  }

  default double putBatchMaxTimeMs() {
    return (double) beanObject().attributes().get("put-batch-max-time-ms");
  }

  default double sinkRecordActiveCount() {
    return (double) beanObject().attributes().get("sink-record-active-count");
  }

  default double sinkRecordActiveCountAvg() {
    return (double) beanObject().attributes().get("sink-record-active-count-avg");
  }

  default double sinkRecordActiveCountMax() {
    return (double) beanObject().attributes().get("sink-record-active-count-max");
  }

  default double sinkRecordReadRate() {
    return (double) beanObject().attributes().get("sink-record-read-rate");
  }

  default double sinkRecordReadTotal() {
    return (double) beanObject().attributes().get("sink-record-read-total");
  }

  default double sinkRecordSendRate() {
    return (double) beanObject().attributes().get("sink-record-send-rate");
  }

  default double sinkRecordSendTotal() {
    return (double) beanObject().attributes().get("sink-record-send-total");
  }
}
