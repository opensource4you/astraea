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

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.function.Function;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.Node;
import org.astraea.common.metrics.MBeanClient;
import org.astraea.common.metrics.broker.NetworkMetrics;

public class RpcGuard implements Guard {
  @Override
  public Collection<Report> run(
      Admin admin, Function<Node, MBeanClient> clients, Changelog changelog) throws Exception {
    Map<String, Protocol> protocols = changelog.protocols();
    return admin.describeCluster().nodes().get().stream()
        .map(node -> checkNode(node, protocols, clients))
        .flatMap(Collection::stream)
        .toList();
  }

  private Collection<Report> checkNode(
      Node node, Map<String, Protocol> protocols, Function<Node, MBeanClient> clients) {
    return Arrays.stream(NetworkMetrics.Request.values())
        .filter(request -> protocols.containsKey(request.metricName().toLowerCase()))
        .flatMap(
            request -> {
              var protocol = protocols.get(request.metricName().toLowerCase());
              var versions = request.versions(clients.apply(node));
              return Report.of(node, protocol, versions).stream();
            })
        .toList();
  }
}
