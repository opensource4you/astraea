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

import java.util.Objects;
import org.astraea.common.EnumInfo;
import org.astraea.common.metrics.BeanObject;
import org.astraea.common.metrics.BeanQuery;
import org.astraea.common.metrics.MBeanClient;

public class ControllerMetrics {
  public enum Controller implements EnumInfo {
    ACTIVE_CONTROLLER_COUNT("ActiveControllerCount"),
    OFFLINE_PARTITIONS_COUNT("OfflinePartitionsCount"),
    PREFERRED_REPLICA_IMBALANCE_COUNT("PreferredReplicaImbalanceCount"),
    GLOBAL_TOPIC_COUNT("GlobalTopicCount"),
    GLOBAL_PARTITION_COUNT("GlobalPartitionCount"),
    ACTIVE_BROKER_COUNT("ActiveBrokerCount"),
    FENCED_BROKER_COUNT("FencedBrokerCount");

    static Controller ofAlias(String alias) {
      return EnumInfo.ignoreCaseEnum(Controller.class, alias);
    }

    private final String metricName;

    Controller(String metricName) {
      this.metricName = metricName;
    }

    public String metricName() {
      return metricName;
    }

    @Override
    public String alias() {
      return metricName();
    }

    @Override
    public String toString() {
      return alias();
    }

    public Gauge fetch(MBeanClient mBeanClient) {
      return new Gauge(
          mBeanClient.bean(
              BeanQuery.builder()
                  .domainName("kafka.controller")
                  .property("type", "KafkaController")
                  .property("name", this.metricName())
                  .build()));
    }

    public static class Gauge implements HasGauge<Integer> {
      private final BeanObject beanObject;

      public Gauge(BeanObject beanObject) {
        this.beanObject = beanObject;
      }

      public String metricsName() {
        return beanObject().properties().get("name");
      }

      @Override
      public Integer value() {
        return (int) Objects.requireNonNull(beanObject().attributes().get("Value"));
      }

      public Controller type() {
        return Controller.ofAlias(metricsName());
      }

      @Override
      public BeanObject beanObject() {
        return beanObject;
      }
    }
  }
}
