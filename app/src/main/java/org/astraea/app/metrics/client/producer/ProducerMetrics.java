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
package org.astraea.app.metrics.client.producer;

import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;
import org.astraea.app.metrics.BeanQuery;
import org.astraea.app.metrics.MBeanClient;
import org.astraea.app.metrics.client.HasNodeMetrics;

public final class ProducerMetrics {

  private static int brokerId(String node) {
    return Integer.parseInt(node.substring(node.indexOf("-") + 1));
  }

  /**
   * node metrics traced by producer
   *
   * @param mBeanClient to query beans
   * @param brokerId broker ids
   * @return key is client id used by producer, and value is node metrics traced by each producer
   */
  public static Map<String, HasNodeMetrics> node(MBeanClient mBeanClient, int brokerId) {
    return mBeanClient
        .queryBeans(
            BeanQuery.builder()
                .domainName("kafka.producer")
                .property("type", "producer-node-metrics")
                .property("node-id", "node-" + brokerId)
                .property("client-id", "*")
                .build())
        .stream()
        .collect(
            Collectors.toUnmodifiableMap(
                b -> b.properties().get("client-id"),
                b -> HasNodeMetrics.of(b, brokerId(b.properties().get("node-id")))));
  }

  /**
   * collect HasProducerNodeMetrics from all producers.
   *
   * @param mBeanClient to query metrics
   * @return key is broker id, and value is associated to broker metrics recorded by all producers
   */
  public static Collection<HasNodeMetrics> nodes(MBeanClient mBeanClient) {
    return mBeanClient
        .queryBeans(
            BeanQuery.builder()
                .domainName("kafka.producer")
                .property("type", "producer-node-metrics")
                .property("node-id", "*")
                .property("client-id", "*")
                .build())
        .stream()
        .map(b -> HasNodeMetrics.of(b, brokerId(b.properties().get("node-id"))))
        .collect(Collectors.toUnmodifiableList());
  }

  /**
   * topic metrics traced by producer
   *
   * @param mBeanClient to query beans
   * @return key is client id used by producer, and value is topic metrics traced by each producer
   */
  public static Collection<HasProducerTopicMetrics> topics(MBeanClient mBeanClient) {
    return mBeanClient
        .queryBeans(
            BeanQuery.builder()
                .domainName("kafka.producer")
                .property("type", "producer-topic-metrics")
                .property("client-id", "*")
                .property("topic", "*")
                .build())
        .stream()
        .map(b -> (HasProducerTopicMetrics) () -> b)
        .collect(Collectors.toUnmodifiableList());
  }

  private ProducerMetrics() {}
}
