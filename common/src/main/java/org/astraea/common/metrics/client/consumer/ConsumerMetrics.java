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
package org.astraea.common.metrics.client.consumer;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import org.astraea.common.Utils;
import org.astraea.common.metrics.AppInfo;
import org.astraea.common.metrics.BeanQuery;
import org.astraea.common.metrics.MBeanClient;
import org.astraea.common.metrics.client.HasNodeMetrics;

public class ConsumerMetrics {

  public static final BeanQuery appInfoQuery =
      BeanQuery.builder()
          .domainName("kafka.consumer")
          .property("type", "app-info")
          .property("client-id", "*")
          .build();

  public static final BeanQuery NODE_QUERY =
      BeanQuery.builder()
          .domainName("kafka.consumer")
          .property("type", "consumer-node-metrics")
          .property("node-id", "*")
          .property("client-id", "*")
          .build();

  public static final BeanQuery COORDINATOR_QUERY =
      BeanQuery.builder()
          .domainName("kafka.consumer")
          .property("type", "consumer-coordinator-metrics")
          .property("client-id", "*")
          .build();

  public static final BeanQuery FETCH_QUERY =
      BeanQuery.builder()
          .domainName("kafka.consumer")
          .property("type", "consumer-fetch-manager-metrics")
          .property("client-id", "*")
          .build();

  public static final BeanQuery CONSUMER_QUERY =
      BeanQuery.builder()
          .domainName("kafka.consumer")
          .property("type", "consumer-metrics")
          .property("client-id", "*")
          .build();
  public static final Collection<BeanQuery> QUERIES =
      Utils.constants(ConsumerMetrics.class, name -> name.endsWith("QUERY"), BeanQuery.class);

  public static List<AppInfo> appInfo(MBeanClient client) {
    return client.beans(appInfoQuery).stream()
        .map(b -> (AppInfo) () -> b)
        .collect(Collectors.toList());
  }
  /**
   * collect HasNodeMetrics from all consumers.
   *
   * @param mBeanClient to query metrics
   * @return key is broker id, and value is associated to broker metrics recorded by all consumers
   */
  public static Collection<HasNodeMetrics> node(MBeanClient mBeanClient) {
    return mBeanClient.beans(NODE_QUERY).stream().map(b -> (HasNodeMetrics) () -> b).toList();
  }

  public static Collection<HasConsumerCoordinatorMetrics> coordinator(MBeanClient mBeanClient) {
    return mBeanClient.beans(COORDINATOR_QUERY).stream()
        .map(b -> (HasConsumerCoordinatorMetrics) () -> b)
        .toList();
  }

  public static Collection<HasConsumerFetchMetrics> fetch(MBeanClient mBeanClient) {
    return mBeanClient.beans(FETCH_QUERY).stream()
        .map(b -> (HasConsumerFetchMetrics) () -> b)
        .toList();
  }

  public static Collection<HasConsumerMetrics> consumer(MBeanClient mBeanClient) {
    return mBeanClient.beans(CONSUMER_QUERY).stream()
        .map(b -> (HasConsumerMetrics) () -> b)
        .toList();
  }
}
