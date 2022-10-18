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
import java.util.function.Function;
import java.util.stream.Collectors;
import org.astraea.common.metrics.AppInfo;
import org.astraea.common.metrics.BeanObject;
import org.astraea.common.metrics.BeanQuery;
import org.astraea.common.metrics.MBeanClient;
import org.astraea.common.metrics.client.HasNodeMetrics;

public final class ProducerMetrics {

  public static List<AppInfo> appInfo(MBeanClient client) {
    return client
        .queryBeans(
            BeanQuery.builder()
                .domainName("kafka.producer")
                .property("type", "app-info")
                .property("client-id", "*")
                .build())
        .stream()
        .map(
            obj ->
                new AppInfo() {
                  @Override
                  public String id() {
                    return beanObject().properties().get("client-id");
                  }

                  @Override
                  public String commitId() {
                    return (String) beanObject().attributes().get("commit-id");
                  }

                  @Override
                  public long startTimeMs() {
                    return (long) beanObject().attributes().get("start-time-ms");
                  }

                  @Override
                  public String version() {
                    return (String) beanObject().attributes().get("version");
                  }

                  @Override
                  public BeanObject beanObject() {
                    return obj;
                  }
                })
        .collect(Collectors.toList());
  }

  /**
   * collect HasProducerNodeMetrics from all producers.
   *
   * @param mBeanClient to query metrics
   * @return key is broker id, and value is associated to broker metrics recorded by all producers
   */
  public static Collection<HasNodeMetrics> nodes(MBeanClient mBeanClient) {
    Function<String, Integer> brokerId =
        node -> Integer.parseInt(node.substring(node.indexOf("-") + 1));
    return mBeanClient
        .queryBeans(
            BeanQuery.builder()
                .domainName("kafka.producer")
                .property("type", "producer-node-metrics")
                .property("node-id", "*")
                .property("client-id", "*")
                .build())
        .stream()
        .map(b -> (HasNodeMetrics) () -> b)
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

  public static Collection<HasProducerMetrics> of(MBeanClient mBeanClient) {
    return mBeanClient
        .queryBeans(
            BeanQuery.builder()
                .domainName("kafka.producer")
                .property("type", "producer-metrics")
                .property("client-id", "*")
                .build())
        .stream()
        .map(b -> (HasProducerMetrics) () -> b)
        .collect(Collectors.toUnmodifiableList());
  }

  private ProducerMetrics() {}
}
