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

public interface ConnectorInfo extends HasBeanObject {

  default String connectorName() {
    return beanObject().properties().get("connector");
  }

  default int taskId() {
    return Integer.parseInt(beanObject().properties().get("task"));
  }

  default String connectorType() {
    return beanObject().properties().get("type");
  }

  default double batchSizeAvg() {
    return (double) beanObject().attributes().get("batch-size-avg");
  }

  default double batchSizeMax() {
    return (double) beanObject().attributes().get("batch-size-max");
  }

  default double offsetCommitAvgTimeMs() {
    return (double) beanObject().attributes().get("offset-commit-avg-time-ms");
  }

  default double offsetCommitMaxTimeMs() {
    return (double) beanObject().attributes().get("offset-commit-max-time-ms");
  }

  default double offsetCommitFailurePercentage() {
    return (double) beanObject().attributes().get("offset-commit-failure-percentage");
  }

  default double offsetCommitSuccessPercentage() {
    return (double) beanObject().attributes().get("offset-commit-success-percentage");
  }

  default double pauseRatio() {
    return (double) beanObject().attributes().get("pause-ratio");
  }

  default String status() {
    return (String) beanObject().attributes().get("status");
  }
}
