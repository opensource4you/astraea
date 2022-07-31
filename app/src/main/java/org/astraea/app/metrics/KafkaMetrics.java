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
package org.astraea.app.metrics;

import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;
import org.astraea.app.metrics.broker.HasValue;
import org.astraea.app.metrics.broker.TotalTimeMs;

public final class KafkaMetrics {

  private KafkaMetrics() {}

  public enum Request {
    Produce,
    FetchConsumer,
    FetchFollower;

    public TotalTimeMs totalTimeMs(MBeanClient mBeanClient) {
      return new TotalTimeMs(
          mBeanClient.queryBean(
              BeanQuery.builder()
                  .domainName("kafka.network")
                  .property("type", "RequestMetrics")
                  .property("request", this.name())
                  .property("name", "TotalTimeMs")
                  .build()));
    }
  }

  public enum ReplicaManager {
    AtMinIsrPartitionCount("AtMinIsrPartitionCount"),
    LeaderCount("LeaderCount"),
    OfflineReplicaCount("OfflineReplicaCount"),
    PartitionCount("PartitionCount"),
    ReassigningPartitions("ReassigningPartitions"),
    UnderMinIsrPartitionCount("UnderMinIsrPartitionCount"),
    UnderReplicatedPartitions("UnderReplicatedPartition");
    private final String metricName;

    ReplicaManager(String name) {
      this.metricName = name;
    }

    public String metricName() {
      return metricName;
    }

    public static ReplicaManager of(String metricName) {
      return Arrays.stream(ReplicaManager.values())
          .filter(metric -> metric.metricName().equalsIgnoreCase(metricName))
          .findFirst()
          .orElseThrow(() -> new IllegalArgumentException("No such metric: " + metricName));
    }

    public Collection<HasBeanObject> fetch(MBeanClient mBeanClient) {
      return mBeanClient
          .queryBeans(
              BeanQuery.builder()
                  .domainName("kafka.server")
                  .property("type", "ReplicaManager")
                  .property("name", metricName)
                  .build())
          .stream()
          .map(HasValue::of)
          .collect(Collectors.toUnmodifiableList());
    }
  }
}
