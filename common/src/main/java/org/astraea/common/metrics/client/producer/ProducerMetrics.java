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
package org.astraea.common.metrics.client.producer;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import org.astraea.common.Utils;
import org.astraea.common.metrics.AppInfo;
import org.astraea.common.metrics.BeanQuery;
import org.astraea.common.metrics.MBeanClient;
import org.astraea.common.metrics.client.HasNodeMetrics;

public final class ProducerMetrics {

  public static final BeanQuery APP_INFO_QUERY =
      BeanQuery.builder()
          .domainName("kafka.producer")
          .property("type", "app-info")
          .property("client-id", "*")
          .build();

  public static final BeanQuery NODE_QUERY =
      BeanQuery.builder()
          .domainName("kafka.producer")
          .property("type", "producer-node-metrics")
          .property("node-id", "*")
          .property("client-id", "*")
          .build();

  public static final BeanQuery TOPIC_QUERY =
      BeanQuery.builder()
          .domainName("kafka.producer")
          .property("type", "producer-topic-metrics")
          .property("client-id", "*")
          .property("topic", "*")
          .build();

  public static final BeanQuery PRODUCER_QUERY =
      BeanQuery.builder()
          .domainName("kafka.producer")
          .property("type", "producer-metrics")
          .property("client-id", "*")
          .build();

  public static final Collection<BeanQuery> QUERIES =
      Utils.constants(ProducerMetrics.class, name -> name.endsWith("QUERY"), BeanQuery.class);

  public static List<AppInfo> appInfo(MBeanClient client) {
    return client.beans(APP_INFO_QUERY).stream()
        .map(b -> (AppInfo) () -> b)
        .collect(Collectors.toList());
  }

  /**
   * collect HasProducerNodeMetrics from all producers.
   *
   * @param mBeanClient to query metrics
   * @return key is broker id, and value is associated to broker metrics recorded by all producers
   */
  public static Collection<HasNodeMetrics> node(MBeanClient mBeanClient) {
    return mBeanClient.beans(NODE_QUERY).stream().map(b -> (HasNodeMetrics) () -> b).toList();
  }

  /**
   * topic metrics traced by producer
   *
   * @param mBeanClient to query beans
   * @return key is client id used by producer, and value is topic metrics traced by each producer
   */
  public static Collection<HasProducerTopicMetrics> topic(MBeanClient mBeanClient) {
    return mBeanClient.beans(TOPIC_QUERY).stream()
        .map(b -> (HasProducerTopicMetrics) () -> b)
        .toList();
  }

  public static Collection<HasProducerMetrics> producer(MBeanClient mBeanClient) {
    return mBeanClient.beans(PRODUCER_QUERY).stream()
        .map(b -> (HasProducerMetrics) () -> b)
        .toList();
  }

  private ProducerMetrics() {}
}
