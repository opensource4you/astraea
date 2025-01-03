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
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.Node;
import org.astraea.common.metrics.AppInfo;
import org.astraea.common.metrics.MBeanClient;
import org.astraea.common.metrics.broker.ServerMetrics;

public class VersionGuard implements Guard {
  private static final Set<String> UNSUPPORTED_VERSIONS =
      Set.of(
          "0.7.0",
          "0.7.1",
          "0.7.2",
          "0.8.0",
          "0.8.1",
          "0.8.1.1",
          "0.8.2-beta",
          "0.8.2.1",
          "0.8.2.2",
          "0.9.0.0",
          "0.9.0.1",
          "0.10.0.0",
          "0.10.0.1",
          "0.10.1.0",
          "0.10.1.1",
          "0.10.2.0",
          "0.10.2.1",
          "0.10.2.2",
          "0.11.0.0",
          "0.11.0.1",
          "0.11.0.2",
          "0.11.0.3",
          "1.0.0",
          "1.0.1",
          "1.0.2",
          "1.1.0",
          "1.1.1",
          "2.0.0",
          "2.0.1");

  @Override
  public Collection<Report> run(
      Admin admin, Function<Node, MBeanClient> clients, Changelog changelog) throws Exception {
    return admin.describeCluster().nodes().get().stream()
        .map(
            node -> {
              var vs =
                  ServerMetrics.appInfo(clients.apply(node)).stream()
                      .map(AppInfo::version)
                      .filter(UNSUPPORTED_VERSIONS::contains)
                      .collect(Collectors.toSet());
              if (vs.isEmpty()) return Report.empty();
              return Report.of(
                  node, "kafka versions: " + vs + " are unsupported due to new baseline is 2.1.0");
            })
        .flatMap(Report::stream)
        .toList();
  }
}
