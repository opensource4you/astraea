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
package org.astraea.common.metrics.broker;

import java.util.List;
import java.util.stream.Collectors;
import org.astraea.common.EnumInfo;
import org.astraea.common.admin.TopicPartition;
import org.astraea.common.metrics.BeanObject;
import org.astraea.common.metrics.BeanQuery;
import org.astraea.common.metrics.MBeanClient;

public final class ClusterMetrics {

  public static final String DOMAIN_NAME = "kafka.cluster";

  public enum Partition implements EnumInfo {
    REPLICAS_COUNT("ReplicasCount");

    private final String metricName;

    Partition(String metricName) {
      this.metricName = metricName;
    }

    @Override
    public String alias() {
      return metricName();
    }

    @Override
    public String toString() {
      return alias();
    }

    public String metricName() {
      return this.metricName;
    }

    public static ClusterMetrics.Partition ofAlias(String alias) {
      return EnumInfo.ignoreCaseEnum(ClusterMetrics.Partition.class, alias);
    }

    public List<PartitionMetric> fetch(MBeanClient client) {
      return client
          .beans(
              BeanQuery.builder()
                  .domainName(DOMAIN_NAME)
                  .property("type", "Partition")
                  .property("topic", "*")
                  .property("partition", "*")
                  .property("name", metricName())
                  .build())
          .stream()
          .map(PartitionMetric::new)
          .collect(Collectors.toUnmodifiableList());
    }
  }

  public static class PartitionMetric implements HasGauge<Integer> {
    private final BeanObject beanObject;

    public PartitionMetric(BeanObject beanObject) {
      this.beanObject = beanObject;
    }

    @Override
    public BeanObject beanObject() {
      return beanObject;
    }

    public TopicPartition topicPartition() {
      return partitionIndex().orElseThrow();
    }
  }

  private ClusterMetrics() {}
}
