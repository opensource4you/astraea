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

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import org.astraea.common.Utils;
import org.astraea.common.metrics.AppInfo;
import org.astraea.common.metrics.BeanQuery;
import org.astraea.common.metrics.MBeanClient;
import org.astraea.common.metrics.client.HasNodeMetrics;
import org.astraea.common.metrics.client.HasSelectorMetrics;

public class ConnectorMetrics {

  public static final BeanQuery SOURCE_TASK_INFO_QUERY =
      BeanQuery.builder()
          .domainName("kafka.connect")
          .property("type", "source-task-metrics")
          .property("connector", "*")
          .property("task", "*")
          .build();

  public static final BeanQuery SINK_TASK_INFO_QUERY =
      BeanQuery.builder()
          .domainName("kafka.connect")
          .property("type", "sink-task-metrics")
          .property("connector", "*")
          .property("task", "*")
          .build();

  public static final BeanQuery TASK_ERROR_QUERY =
      BeanQuery.builder()
          .domainName("kafka.connect")
          .property("type", "task-error-metrics")
          .property("connector", "*")
          .property("task", "*")
          .build();

  public static final BeanQuery CONNECTOR_TASK_INFO_QUERY =
      BeanQuery.builder()
          .domainName("kafka.connect")
          .property("type", "connector-task-metrics")
          .property("connector", "*")
          .property("task", "*")
          .build();

  public static final BeanQuery APP_INFO_QUERY =
      BeanQuery.builder()
          .domainName("kafka.connect")
          .property("type", "app-info")
          .property("client-id", "*")
          .build();

  public static final BeanQuery CONNECTOR_INFO_QUERY =
      BeanQuery.builder()
          .domainName("kafka.connect")
          .property("type", "connector-metrics")
          .property("connector", "*")
          .build();

  public static final BeanQuery COORDINATOR_INFO_QUERY =
      BeanQuery.builder()
          .domainName("kafka.connect")
          .property("type", "connect-coordinator-metrics")
          .property("client-id", "*")
          .build();

  public static final BeanQuery NODE_INFO_QUERY =
      BeanQuery.builder()
          .domainName("kafka.connect")
          .property("type", "connect-node-metrics")
          .property("client-id", "*")
          .property("node-id", "*")
          .build();

  public static final BeanQuery WORKER_CONNECTOR_INFO_QUERY =
      BeanQuery.builder()
          .domainName("kafka.connect")
          .property("type", "connect-worker-metrics")
          .property("connector", "*")
          .build();

  public static final BeanQuery CONNECTOR_QUERY =
      BeanQuery.builder()
          .domainName("kafka.connect")
          .property("type", "connect-metrics")
          .property("client-id", "*")
          .build();

  public static final BeanQuery WORKER_INFO_QUERY =
      BeanQuery.builder()
          .domainName("kafka.connect")
          .property("type", "connect-worker-metrics")
          .build();
  public static final BeanQuery WORKER_REBALANCE_INFO_QUERY =
      BeanQuery.builder()
          .domainName("kafka.connect")
          .property("type", "connect-worker-rebalance-metrics")
          .build();

  public static final Collection<BeanQuery> QUERIES =
      Utils.constants(ConnectorMetrics.class, name -> name.endsWith("QUERY"), BeanQuery.class);

  public static List<SourceTaskInfo> sourceTaskInfo(MBeanClient client) {
    return client.beans(SOURCE_TASK_INFO_QUERY).stream()
        .map(b -> (SourceTaskInfo) () -> b)
        .collect(Collectors.toUnmodifiableList());
  }

  public static List<SinkTaskInfo> sinkTaskInfo(MBeanClient client) {
    return client.beans(SINK_TASK_INFO_QUERY).stream()
        .map(b -> (SinkTaskInfo) () -> b)
        .collect(Collectors.toUnmodifiableList());
  }

  public static List<TaskError> taskError(MBeanClient client) {
    return client.beans(TASK_ERROR_QUERY).stream()
        .map(b -> (TaskError) () -> b)
        .collect(Collectors.toUnmodifiableList());
  }

  public static List<ConnectorTaskInfo> connectorTaskInfo(MBeanClient client) {
    return client.beans(CONNECTOR_TASK_INFO_QUERY).stream()
        .map(b -> (ConnectorTaskInfo) () -> b)
        .collect(Collectors.toUnmodifiableList());
  }

  public static List<AppInfo> appInfo(MBeanClient client) {
    return client.beans(APP_INFO_QUERY).stream()
        .map(b -> (AppInfo) () -> b)
        .collect(Collectors.toList());
  }

  public static List<ConnectCoordinatorInfo> coordinatorInfo(MBeanClient client) {
    return client.beans(COORDINATOR_INFO_QUERY).stream()
        .map(b -> (ConnectCoordinatorInfo) () -> b)
        .collect(Collectors.toUnmodifiableList());
  }

  public static List<ConnectorInfo> connectorInfo(MBeanClient client) {
    return client.beans(CONNECTOR_INFO_QUERY).stream()
        .map(b -> (ConnectorInfo) () -> b)
        .collect(Collectors.toUnmodifiableList());
  }

  public static List<ConnectWorkerRebalanceInfo> workerRebalanceInfo(MBeanClient client) {
    return client.beans(WORKER_REBALANCE_INFO_QUERY).stream()
        .map(b -> (ConnectWorkerRebalanceInfo) () -> b)
        .collect(Collectors.toUnmodifiableList());
  }

  public static List<HasNodeMetrics> nodeInfo(MBeanClient client) {
    return client.beans(NODE_INFO_QUERY).stream()
        .map(b -> (HasNodeMetrics) () -> b)
        .collect(Collectors.toUnmodifiableList());
  }

  public static List<ConnectWorkerInfo> workerInfo(MBeanClient client) {
    return client.beans(WORKER_INFO_QUERY).stream()
        .map(b -> (ConnectWorkerInfo) () -> b)
        .collect(Collectors.toUnmodifiableList());
  }

  public static List<ConnectWorkerConnectorInfo> workerConnectorInfo(MBeanClient client) {
    return client.beans(WORKER_CONNECTOR_INFO_QUERY).stream()
        .map(b -> (ConnectWorkerConnectorInfo) () -> b)
        .collect(Collectors.toUnmodifiableList());
  }

  public static List<HasSelectorMetrics> connector(MBeanClient client) {
    return client.beans(CONNECTOR_QUERY).stream()
        .map(b -> (HasSelectorMetrics) () -> b)
        .collect(Collectors.toUnmodifiableList());
  }
}
