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
package org.astraea.app.checker;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.config.ConfigResource;
import org.astraea.common.metrics.MBeanClient;

public class ConfigGuard implements Guard {
  @Override
  public Collection<Report> run(
      Admin admin, Function<Node, MBeanClient> clients, Changelog changelog) throws Exception {
    var nameToCheckerConfig = changelog.configs();
    var nodes = admin.describeCluster().nodes().get();
    var idToNode =
        nodes.stream()
            .collect(Collectors.toMap(node -> String.valueOf(node.id()), Function.identity()));

    var brokerReports =
        processConfigs(
            nodes.stream()
                .map(
                    node ->
                        new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(node.id())))
                .toList(),
            admin,
            nameToCheckerConfig,
            resource -> idToNode.get(resource.name()));

    var topicReports =
        processConfigs(
            admin.listTopics().listings().get().stream()
                .map(topic -> new ConfigResource(ConfigResource.Type.TOPIC, topic.name()))
                .toList(),
            admin,
            nameToCheckerConfig,
            resource -> null);

    return Stream.concat(brokerReports.stream(), topicReports.stream())
        .filter(report -> !report.why().isEmpty())
        .toList();
  }

  private List<Report> processConfigs(
      List<ConfigResource> resources,
      Admin admin,
      Map<String, org.astraea.app.checker.Config> nameToCheckerConfig,
      Function<ConfigResource, Node> nodeResolver)
      throws Exception {
    return admin.describeConfigs(resources).all().get().entrySet().stream()
        .flatMap(
            entry ->
                entry.getValue().entries().stream()
                    .filter(config -> nameToCheckerConfig.containsKey(config.name()))
                    .map(
                        config ->
                            nameToCheckerConfig
                                .get(config.name())
                                .value()
                                .filter(defaultValue -> !config.value().equals(defaultValue))
                                .map(
                                    __ ->
                                        Report.of(
                                            nodeResolver.apply(entry.getKey()),
                                            String.format(
                                                "%s has been removed from AK 4.0", config.name())))
                                .orElseGet(Report::empty)))
        .toList();
  }
}
