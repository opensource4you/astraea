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
package org.astraea.app.web;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.astraea.common.Configuration;
import org.astraea.common.Utils;
import org.astraea.common.admin.Admin;
import org.astraea.common.cost.CostFunction;
import org.astraea.common.json.TypeRef;
import org.astraea.common.metrics.BeanObject;
import org.astraea.common.metrics.BeanQuery;
import org.astraea.common.metrics.MBeanClient;
import org.astraea.common.metrics.collector.MetricSensor;

public class MetricSensorHandler implements Handler {
  private final Admin admin;
  private final Function<Integer, Integer> jmxPorts;
  private final Collection<MetricSensor> sensors;

  MetricSensorHandler(
      Admin admin, Function<Integer, Integer> jmxPorts, Collection<MetricSensor> sensors) {
    this.admin = admin;
    this.jmxPorts = jmxPorts;
    this.sensors = sensors;
  }

  @Override
  public CompletionStage<Response> post(Channel channel) {
    var metricSensorPostRequest = channel.request(TypeRef.of(MetricSensorPostRequest.class));
    var costs = costs(metricSensorPostRequest.costs);
    if (!costs.isEmpty()) sensors.clear();
    costs.forEach(costFunction -> costFunction.metricSensor().ifPresent(sensors::add));
    return CompletableFuture.completedFuture(new PostResponse(metricSensorPostRequest.costs));
  }

  @Override
  public CompletionStage<Response> get(Channel channel) {
    var builder = BeanQuery.builder().usePropertyListPattern().properties(channel.queries());
    return admin
        .brokers()
        .thenApply(
            brokers ->
                new NodeBeans(
                    brokers.stream()
                        .map(
                            b -> {
                              try (var client =
                                  MBeanClient.jndi(b.host(), jmxPorts.apply(b.id()))) {
                                return new NodeBean(
                                    b.host(),
                                    client.beans(builder.build()).stream()
                                        .map(Bean::new)
                                        .collect(Collectors.toUnmodifiableList()));
                              }
                            })
                        .collect(Collectors.toUnmodifiableList())));
  }

  private static Set<CostFunction> costs(Set<String> costs) {
    if (costs.isEmpty()) throw new IllegalArgumentException("costs is not specified");
    return Utils.costFunctions(costs, CostFunction.class, Configuration.EMPTY);
  }

  static class Property implements Response {
    final String key;
    final String value;

    Property(String key, String value) {
      this.key = key;
      this.value = value;
    }
  }

  static class Attribute implements Response {
    final String key;
    final String value;

    Attribute(String key, String value) {
      this.key = key;
      this.value = value;
    }
  }

  static class Bean implements Response {
    final String domainName;
    final List<Property> properties;
    final List<Attribute> attributes;

    Bean(BeanObject obj) {
      this.domainName = obj.domainName();
      this.properties =
          obj.properties().entrySet().stream()
              .map(e -> new Property(e.getKey(), e.getValue()))
              .collect(Collectors.toUnmodifiableList());
      this.attributes =
          obj.attributes().entrySet().stream()
              .map(e -> new Attribute(e.getKey(), e.getValue().toString()))
              .collect(Collectors.toUnmodifiableList());
    }
  }

  static class NodeBean implements Response {
    final String host;
    final List<Bean> beans;

    NodeBean(String host, List<Bean> beans) {
      this.host = host;
      this.beans = beans;
    }
  }

  static class NodeBeans implements Response {
    final List<NodeBean> nodeBeans;

    NodeBeans(List<NodeBean> nodeBeans) {
      this.nodeBeans = nodeBeans;
    }
  }

  static class MetricSensorPostRequest implements Request {
    Set<String> costs =
        Set.of(
            "org.astraea.common.cost.ReplicaLeaderCost",
            "org.astraea.common.cost.NetworkIngressCost");
  }

  static class PostResponse implements Response {
    final Set<String> costs;

    PostResponse(Set<String> costs) {
      this.costs = costs;
    }
  }
}
