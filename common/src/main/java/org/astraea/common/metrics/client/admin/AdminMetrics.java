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
package org.astraea.common.metrics.client.admin;

import java.util.Collection;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.astraea.common.metrics.BeanQuery;
import org.astraea.common.metrics.MBeanClient;
import org.astraea.common.metrics.client.HasNodeMetrics;

public class AdminMetrics {
  /**
   * collect HasNodeMetrics from all consumers.
   *
   * @param mBeanClient to query metrics
   * @return key is broker id, and value is associated to broker metrics recorded by all consumers
   */
  public static Collection<HasNodeMetrics> nodes(MBeanClient mBeanClient) {
    Function<String, Integer> brokerId =
        node -> Integer.parseInt(node.substring(node.indexOf("-") + 1));
    return mBeanClient
        .queryBeans(
            BeanQuery.builder()
                .domainName("kafka.admin.client")
                .property("type", "admin-client-node-metrics")
                .property("node-id", "*")
                .property("client-id", "*")
                .build())
        .stream()
        .map(b -> (HasNodeMetrics) () -> b)
        .collect(Collectors.toUnmodifiableList());
  }

  public static Collection<HasAdminMetrics> of(MBeanClient mBeanClient) {
    return mBeanClient
        .queryBeans(
            BeanQuery.builder()
                .domainName("kafka.admin.client")
                .property("type", "admin-client-metrics")
                .property("client-id", "*")
                .build())
        .stream()
        .map(b -> (HasAdminMetrics) () -> b)
        .collect(Collectors.toUnmodifiableList());
  }
}
