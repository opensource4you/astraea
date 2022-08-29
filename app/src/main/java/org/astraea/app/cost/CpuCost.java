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
package org.astraea.app.cost;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.astraea.app.admin.ClusterBean;
import org.astraea.app.admin.ClusterInfo;
import org.astraea.app.admin.ReplicaInfo;
import org.astraea.app.metrics.HasBeanObject;
import org.astraea.app.metrics.collector.Fetcher;
import org.astraea.app.metrics.platform.HostMetrics;
import org.astraea.app.metrics.platform.OperatingSystemInfo;

/**
 * The result is computed by "OperatingSystemInfo.systemCpuLoad".
 * "OperatingSystemInfo.systemCpuLoad" responds to the cpu usage of brokers.
 */
public class CpuCost implements HasBrokerCost {

  @Override
  public BrokerCost brokerCost(
      ClusterInfo<? extends ReplicaInfo> clusterInfo, ClusterBean clusterBean) {
    var cpuCosts =
        clusterBean.all().entrySet().stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    entry ->
                        entry.getValue().stream()
                            .filter(obj -> obj instanceof OperatingSystemInfo)
                            .map(obj -> (OperatingSystemInfo) obj)
                            .sorted(
                                Comparator.comparingLong(HasBeanObject::createdTimestamp)
                                    .reversed())
                            .map(OperatingSystemInfo::systemCpuLoad)
                            .findFirst()
                            .orElse(0D)));

    return () -> cpuCosts;
  }

  @Override
  public Optional<Fetcher> fetcher() {
    return Optional.of(client -> List.of(HostMetrics.operatingSystem(client)));
  }
}
