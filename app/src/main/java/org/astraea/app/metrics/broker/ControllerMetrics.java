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
package org.astraea.app.metrics.broker;

import java.util.Arrays;
import org.astraea.app.metrics.BeanObject;
import org.astraea.app.metrics.BeanQuery;
import org.astraea.app.metrics.MBeanClient;

public class ControllerMetrics {
  public enum Controller {
    ACTIVE_CONTROLLER_COUNT("ActiveControllerCount"),
    OFFLINE_PARTITIONS_COUNT("OfflinePartitionsCount"),
    PREFERRED_REPLICA_IMBALANCE_COUNT("PreferredReplicaImbalanceCount"),
    CONTROLLER_STATE("ControllerState"),
    GLOBAL_TOPIC_COUNT("GlobalTopicCount"),
    GLOBAL_PARTITION_COUNT("GlobalPartitionCount"),
    TOPICS_TO_DELETE_COUNT("TopicsToDeleteCount"),
    REPLICAS_TO_DELETE_COUNT("ReplicasToDeleteCount"),
    TOPICS_INELIGIBLE_TO_DELETE_COUNT("TopicsIneligibleToDeleteCount"),
    REPLICAS_INELIGIBLE_TO_DELETE_COUNT("ReplicasIneligibleToDeleteCount"),
    ACTIVE_BROKER_COUNT("ActiveBrokerCount"),
    FENCED_BROKER_COUNT("FencedBrokerCount");

    static Controller of(String metricName) {
      return Arrays.stream(Controller.values())
          .filter(metric -> metric.metricName().equalsIgnoreCase(metricName))
          .findFirst()
          .orElseThrow(() -> new IllegalArgumentException("No such metric: " + metricName));
    }

    private final String metricName;

    Controller(String metricName) {
      this.metricName = metricName;
    }

    public String metricName() {
      return metricName;
    }

    public Meter fetch(MBeanClient mBeanClient) {
      return new Meter(
          mBeanClient.queryBean(
              BeanQuery.builder()
                  .domainName("kafka.controller")
                  .property("type", "KafkaController")
                  .property("name", this.metricName())
                  .build()));
    }

    public static class Meter implements HasValue {
      private final BeanObject beanObject;

      public Meter(BeanObject beanObject) {
        this.beanObject = beanObject;
      }

      public String metricsName() {
        return beanObject().properties().get("name");
      }

      public Controller type() {
        return Controller.of(metricsName());
      }

      @Override
      public BeanObject beanObject() {
        return beanObject;
      }
    }
  }
}
