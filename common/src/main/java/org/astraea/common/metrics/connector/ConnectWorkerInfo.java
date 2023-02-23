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
public interface ConnectWorkerInfo extends HasBeanObject {

  default double connectorCount() {
    return (double) beanObject().attributes().get("connector-count");
  }

  default double connectorStartupAttemptsTotal() {
    return (double) beanObject().attributes().get("connector-startup-attempts-total");
  }

  default double connectorStartupFailurePercentage() {
    return (double) beanObject().attributes().get("connector-startup-failure-percentage");
  }

  default double connectorStartupFailureTotal() {
    return (double) beanObject().attributes().get("connector-startup-failure-total");
  }

  default double connectorStartupSuccessPercentage() {
    return (double) beanObject().attributes().get("connector-startup-success-percentage");
  }

  default double connectorStartupSuccessTotal() {
    return (double) beanObject().attributes().get("connector-startup-success-total");
  }

  default double taskCount() {
    return (double) beanObject().attributes().get("task-count");
  }

  default double taskStartupAttemptsTotal() {
    return (double) beanObject().attributes().get("task-startup-attempts-total");
  }

  default double taskStartupFailurePercentage() {
    return (double) beanObject().attributes().get("task-startup-failure-percentage");
  }

  default double taskStartupFailureTotal() {
    return (double) beanObject().attributes().get("task-startup-failure-total");
  }

  default double taskStartupSuccessPercentage() {
    return (double) beanObject().attributes().get("task-startup-success-percentage");
  }

  default double taskStartupSuccessTotal() {
    return (double) beanObject().attributes().get("task-startup-success-total");
  }
}
