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

    public Gauge fetch(MBeanClient mBeanClient) {
      return new Gauge(
          mBeanClient.queryBean(
              BeanQuery.builder()
                  .domainName("kafka.controller")
                  .property("type", "KafkaController")
                  .property("name", this.metricName())
                  .build()));
    }

    public static class Gauge implements HasGauge {
      private final BeanObject beanObject;

      public Gauge(BeanObject beanObject) {
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

  public enum ControllerState {
    UNCLEAN_LEADER_ELECTION_ENABLE_RATE_AND_TIME_MS("UncleanLeaderElectionEnableRateAndTimeMs"),
    TOPIC_DELETION_RATE_AND_TIME_MS("TopicDeletionRateAndTimeMs"),
    LIST_PARTITION_REASSIGNMENT_RATE_AND_TIME_MS("ListPartitionReassignmentRateAndTimeMs"),
    TOPIC_CHANGE_RATE_AND_TIME_MS("TopicChangeRateAndTimeMs"),
    UPDATE_FEATURES_RATE_AND_TIME_MS("UpdateFeaturesRateAndTimeMs"),
    ISR_CHANGE_RATE_AND_TIME_MS("IsrChangeRateAndTimeMs"),
    CONTROLLED_SHUTDOWN_RATE_AND_TIME_MS("ControlledShutdownRateAndTimeMs"),
    PARTITION_REASSIGNMENT_RATE_AND_TIME_MS("PartitionReassignmentRateAndTimeMs"),
    LEADER_AND_ISR_RESPONSE_RECEIVED_RATE_AND_TIME_MS("LeaderAndIsrResponseReceivedRateAndTimeMs"),
    MANUAL_LEADER_BALANCE_RATE_AND_TIME_MS("ManualLeaderBalanceRateAndTimeMs"),
    LEADER_ELECTION_RATE_AND_TIME_MS("LeaderElectionRateAndTimeMs"),
    CONTROLLER_CHANGE_RATE_AND_TIME_MS("ControllerChangeRateAndTimeMs"),
    LOG_DIR_CHANGE_RATE_AND_TIME_MS("LogDirChangeRateAndTimeMs"),
    TOPIC_UNCLEAN_LEADER_ELECTION_ENABLE_RATE_AND_TIME_MS(
        "TopicUncleanLeaderElectionEnableRateAndTimeMs"),
    AUTO_LEADER_BALANCE_RATE_AND_TIME_MS("AutoLeaderBalanceRateAndTimeMs"),
    CONTROLLER_SHUTDOWN_RATE_AND_TIME_MS("ControllerShutdownRateAndTimeMs");

    /** Most of ControllerState metrics is Timer , this is Meter. */
    private static final String UNCLEAN_LEADER_ELECTIONS_PER_SEC = "UncleanLeaderElectionsPerSec";

    static ControllerState of(String metricName) {
      return Arrays.stream(ControllerState.values())
          .filter(metric -> metric.metricName().equalsIgnoreCase(metricName))
          .findFirst()
          .orElseThrow(() -> new IllegalArgumentException("No such metric: " + metricName));
    }

    private final String metricName;

    ControllerState(String metricName) {
      this.metricName = metricName;
    }

    public String metricName() {
      return metricName;
    }

    public static Meter getUncleanLeaderElectionsPerSec(MBeanClient mBeanClient) {
      return new Meter(
          mBeanClient.queryBean(
              BeanQuery.builder()
                  .domainName("kafka.controller")
                  .property("type", "ControllerStats")
                  .property("name", UNCLEAN_LEADER_ELECTIONS_PER_SEC)
                  .build()));
    }

    public Timer fetch(MBeanClient mBeanClient) {
      return new Timer(
          mBeanClient.queryBean(
              BeanQuery.builder()
                  .domainName("kafka.controller")
                  .property("type", "ControllerStats")
                  .property("name", this.metricName())
                  .build()));
    }

    public static class Timer implements HasTimer {
      private final BeanObject beanObject;

      public Timer(BeanObject beanObject) {
        this.beanObject = beanObject;
      }

      public String metricsName() {
        return beanObject().properties().get("name");
      }

      public ControllerState type() {
        return ControllerState.of(metricsName());
      }

      @Override
      public BeanObject beanObject() {
        return beanObject;
      }
    }

    public static class Meter implements HasMeter {
      private final BeanObject beanObject;

      public Meter(BeanObject beanObject) {
        this.beanObject = beanObject;
      }

      @Override
      public BeanObject beanObject() {
        return beanObject;
      }
    }
  }
}
