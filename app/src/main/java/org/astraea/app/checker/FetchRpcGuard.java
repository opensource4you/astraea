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

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.Node;
import org.astraea.common.metrics.MBeanClient;
import org.astraea.common.metrics.broker.NetworkMetrics;

import java.util.Collection;
import java.util.function.Function;

public class FetchRpcGuard implements Guard {
  @Override
  public Collection<Report> run(
      Admin admin, Function<Node, MBeanClient> clients, Changelog changelog) throws Exception {
    return admin.describeCluster().nodes().get().stream()
        .map(
            node -> {
              var protocol =
                  changelog
                      .protocols()
                      .get(NetworkMetrics.Request.FETCH.metricName().toLowerCase());
              if (protocol == null) return Report.empty();
              var versions = NetworkMetrics.Request.FETCH.versions(clients.apply(node));
              return Report.of(node, protocol, versions);
            })
        .flatMap(Report::stream)
        .toList();
  }
}
