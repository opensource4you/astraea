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
import org.astraea.app.metrics.KafkaMetrics;
import org.astraea.app.metrics.collector.Fetcher;
import org.astraea.app.metrics.platform.HasJvmMemory;

public class MemoryCost extends Periodic<Map<Integer, Double>> implements HasBrokerCost {

  /**
   * The result is computed by "HasJvmMemory.getUsed/getMax". "HasJvmMemory.getUsed/getMax" responds
   * to the memory usage of brokers.
   */
  @Override
  public BrokerCost brokerCost(ClusterInfo clusterInfo, ClusterBean clusterBean) {
    var memoryCosts =
        clusterBean.all().entrySet().stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    entry ->
                        entry.getValue().stream()
                            .filter(obj -> obj instanceof HasJvmMemory)
                            .map(obj -> (HasJvmMemory) obj)
                            .sorted(Comparator.comparing(HasJvmMemory::createdTimestamp).reversed())
                            .map(
                                obj ->
                                    (double) obj.heapMemoryUsage().getUsed()
                                        / (double) obj.heapMemoryUsage().getMax())
                            .findFirst()
                            .orElse(0D)));

    return () -> memoryCosts;
  }

  @Override
  public Optional<Fetcher> fetcher() {
    return Optional.of(client -> List.of(KafkaMetrics.Host.jvmMemory(client)));
  }
}
