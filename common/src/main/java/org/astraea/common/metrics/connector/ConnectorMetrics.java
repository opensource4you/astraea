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

import java.util.List;
import java.util.stream.Collectors;
import org.astraea.common.metrics.BeanQuery;
import org.astraea.common.metrics.MBeanClient;

public class ConnectorMetrics {

  public static List<SourceTaskInfo> sourceTaskInfo(MBeanClient client) {
    return client
        .queryBeans(
            BeanQuery.builder()
                .domainName("kafka.connect")
                .property("type", "source-task-metrics")
                .property("connector", "*")
                .property("task", "*")
                .build())
        .stream()
        .map(b -> (SourceTaskInfo) () -> b)
        .collect(Collectors.toUnmodifiableList());
  }

  public static List<SinkTaskInfo> sinkTaskInfo(MBeanClient client) {
    return client
        .queryBeans(
            BeanQuery.builder()
                .domainName("kafka.connect")
                .property("type", "sink-task-metrics")
                .property("connector", "*")
                .property("task", "*")
                .build())
        .stream()
        .map(b -> (SinkTaskInfo) () -> b)
        .collect(Collectors.toUnmodifiableList());
  }

  public static List<TaskError> taskError(MBeanClient client) {
    return client
        .queryBeans(
            BeanQuery.builder()
                .domainName("kafka.connect")
                .property("type", "task-error-metrics")
                .property("connector", "*")
                .property("task", "*")
                .build())
        .stream()
        .map(b -> (TaskError) () -> b)
        .collect(Collectors.toUnmodifiableList());
  }

  public static List<ConnectorInfo> connectorInfo(MBeanClient client) {
    return client
        .queryBeans(
            BeanQuery.builder()
                .domainName("kafka.connect")
                .property("type", "connector-task-metrics")
                .property("connector", "*")
                .property("task", "*")
                .build())
        .stream()
        .map(b -> (ConnectorInfo) () -> b)
        .collect(Collectors.toUnmodifiableList());
  }
}
