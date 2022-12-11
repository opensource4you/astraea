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
public interface SourceTaskError extends HasBeanObject {

  default String connectorName() {
    return beanObject().properties().get("connector");
  }

  default int taskId() {
    return Integer.parseInt(beanObject().properties().get("task"));
  }

  default double deadletterqueueProduceFailures() {
    return (double) beanObject().attributes().get("deadletterqueue-produce-failures");
  }

  default double deadletterqueueProduceRequests() {
    return (double) beanObject().attributes().get("deadletterqueue-produce-requests");
  }

  default long lastErrorTimestamp() {
    return (long) beanObject().attributes().get("last-error-timestamp");
  }

  default double totalErrorsLogged() {
    return (double) beanObject().attributes().get("total-errors-logged");
  }

  default double totalRecordErrors() {
    return (double) beanObject().attributes().get("total-record-errors");
  }

  default double totalRecordFailures() {
    return (double) beanObject().attributes().get("total-record-failures");
  }

  default double totalRecordsSkipped() {
    return (double) beanObject().attributes().get("total-records-skipped");
  }

  default double totalRetries() {
    return (double) beanObject().attributes().get("total-retries");
  }
}
