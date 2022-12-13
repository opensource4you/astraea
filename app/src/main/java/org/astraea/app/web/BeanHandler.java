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

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.astraea.common.admin.Admin;
import org.astraea.common.metrics.BeanObject;
import org.astraea.common.metrics.BeanQuery;
import org.astraea.common.metrics.MBeanClient;

public class BeanHandler implements Handler {
  private final Admin admin;
  private final Function<Integer, Optional<Integer>> jmxPorts;

  BeanHandler(Admin admin, Function<Integer, Optional<Integer>> jmxPorts) {
    this.admin = admin;
    this.jmxPorts = jmxPorts;
  }

  @Override
  public CompletionStage<Response> get(Channel channel) {
    var builder = BeanQuery.builder().usePropertyListPattern().properties(channel.queries());
    return admin
        .brokers()
        .thenApply(
            brokers ->
                brokers.stream()
                    .flatMap(
                        b ->
                            jmxPorts
                                .apply(b.id())
                                .map(port -> MBeanClient.jndi(b.host(), port))
                                .stream())
                    .collect(Collectors.toList()))
        .thenApply(
            clients ->
                new NodeBeans(
                    clients.stream()
                        .map(
                            c -> {
                              try (c) {
                                return new NodeBean(
                                    c.host(),
                                    c.queryBeans(builder.build()).stream()
                                        .map(Bean::new)
                                        .collect(Collectors.toUnmodifiableList()));
                              }
                            })
                        .collect(Collectors.toUnmodifiableList())));
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
}
