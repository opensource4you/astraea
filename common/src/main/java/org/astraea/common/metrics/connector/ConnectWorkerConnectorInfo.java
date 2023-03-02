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
public interface ConnectWorkerConnectorInfo extends HasBeanObject {
  default String connectorName() {
    return beanObject().properties().get("connector");
  }

  default long connectorDestroyedTaskCount() {
    return (long) beanObject().attributes().get("connector-destroyed-task-count");
  }

  default long connectorFailedTaskCount() {
    return (long) beanObject().attributes().get("connector-failed-task-count");
  }

  default long connectorPausedTaskCount() {
    return (long) beanObject().attributes().get("connector-paused-task-count");
  }

  default long connectorRestartingTaskCount() {
    return (long) beanObject().attributes().get("connector-restarting-task-count");
  }

  default long connectorRunningTaskCount() {
    return (long) beanObject().attributes().get("connector-running-task-count");
  }

  default long connectorTotalTaskCount() {
    return (long) beanObject().attributes().get("connector-total-task-count");
  }

  default long connectorUnassignedTaskCount() {
    return (long) beanObject().attributes().get("connector-unassigned-task-count");
  }
}
