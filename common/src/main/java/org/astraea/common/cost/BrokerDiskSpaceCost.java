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
package org.astraea.common.cost;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.astraea.common.Configuration;
import org.astraea.common.DataSize;
import org.astraea.common.admin.ClusterBean;
import org.astraea.common.admin.ClusterInfo;

public class BrokerDiskSpaceCost implements HasMoveCost {

  public static final String BROKER_COST_LIMIT_KEY = "max.broker.total.disk.space";
  public static final String BROKER_PATH_COST_LIMIT_KEY = "max.broker.path.disk.space";
  private final Map<Integer, DataSize> brokerMoveCostLimit;
  private final Map<BrokerPath, DataSize> diskMoveCostLimit;

  public BrokerDiskSpaceCost(Configuration configuration) {
    this.diskMoveCostLimit = diskMoveCostLimit(configuration);
    this.brokerMoveCostLimit = brokerMoveCostLimit(configuration);
  }

  @Override
  public MoveCost moveCost(ClusterInfo before, ClusterInfo after, ClusterBean clusterBean) {
    if (CostUtils.brokerDiskUsageSizeOverflow(before, after, brokerMoveCostLimit))
      return () -> true;
    if (CostUtils.brokerPathDiskUsageSizeOverflow(before, after, diskMoveCostLimit))
      return () -> true;
    return () -> false;
  }

  private Map<BrokerPath, DataSize> diskMoveCostLimit(Configuration configuration) {
    return configuration
        .string(BROKER_PATH_COST_LIMIT_KEY)
        .map(
            s ->
                Arrays.stream(s.split(","))
                    .map(
                        idAndPath -> {
                          var brokerPathAndLimit = idAndPath.split(":");
                          var brokerPath = brokerPathAndLimit[0].split("-");
                          return Map.entry(
                              BrokerPath.of(Integer.parseInt(brokerPath[0]), brokerPath[1]),
                              DataSize.of(brokerPathAndLimit[1]));
                        })
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)))
        .orElse(Map.of());
  }

  private Map<Integer, DataSize> brokerMoveCostLimit(Configuration configuration) {
    return configuration
        .string(BROKER_COST_LIMIT_KEY)
        .map(
            s ->
                Arrays.stream(s.split(","))
                    .map(
                        idAndPath -> {
                          var brokerAndLimit = idAndPath.split(":");
                          return Map.entry(
                              Integer.parseInt(brokerAndLimit[0]), DataSize.of(brokerAndLimit[1]));
                        })
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)))
        .orElse(Map.of());
  }

  public static class BrokerPath {

    private final int broker;
    private final String path;

    public static BrokerPath of(int broker, String path) {
      return new BrokerPath(broker, path);
    }

    public BrokerPath(int broker, String path) {
      this.broker = broker;
      this.path = path;
    }

    public int broker() {
      return broker;
    }

    public String path() {
      return path;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      BrokerPath that = (BrokerPath) o;
      return broker == that.broker && Objects.equals(path, that.path);
    }

    @Override
    public int hashCode() {
      return Objects.hash(broker, path);
    }
  }
}
