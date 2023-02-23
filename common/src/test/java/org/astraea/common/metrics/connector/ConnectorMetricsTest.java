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
package org.astraea.common.metrics.connector;

import org.astraea.common.connector.ConnectorClient;
import org.astraea.common.metrics.MBeanClient;
import org.astraea.it.Service;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ConnectorMetricsTest {
  private static final Service SERVICE =
      Service.builder().numberOfWorkers(1).numberOfBrokers(1).build();

  @AfterAll
  static void closeService() {
    SERVICE.close();
  }

  @Test
  void testMetrics() {
    ConnectorClient.builder().url(SERVICE.workerUrl()).build();

    var m0 = ConnectorMetrics.appInfo(MBeanClient.local());
    Assertions.assertNotEquals(0, m0.size());
    m0.forEach(
        m -> {
          Assertions.assertDoesNotThrow(m::commitId);
          Assertions.assertDoesNotThrow(m::startTimeMs);
          Assertions.assertDoesNotThrow(m::version);
        });

    var m1 = ConnectorMetrics.coordinatorInfo(MBeanClient.local());
    Assertions.assertNotEquals(0, m1.size());
    m1.forEach(
        m -> {
          Assertions.assertDoesNotThrow(m::failedRebalanceRatePerHour);
          Assertions.assertDoesNotThrow(m::failedRebalanceTotal);
          Assertions.assertDoesNotThrow(m::heartbeatRate);
          Assertions.assertDoesNotThrow(m::heartbeatResponseTimeMax);
          Assertions.assertDoesNotThrow(m::heartbeatTotal);
          Assertions.assertDoesNotThrow(m::joinRate);
          Assertions.assertDoesNotThrow(m::joinTimeAvg);
          Assertions.assertDoesNotThrow(m::joinTimeMax);
          Assertions.assertDoesNotThrow(m::joinTotal);
          Assertions.assertDoesNotThrow(m::lastHeartbeatSecondsAgo);
          Assertions.assertDoesNotThrow(m::lastRebalanceSecondsAgo);
          Assertions.assertDoesNotThrow(m::rebalanceLatencyAvg);
          Assertions.assertDoesNotThrow(m::rebalanceLatencyMax);
          Assertions.assertDoesNotThrow(m::rebalanceLatencyTotal);
          Assertions.assertDoesNotThrow(m::rebalanceRatePerHour);
          Assertions.assertDoesNotThrow(m::rebalanceTotal);
          Assertions.assertDoesNotThrow(m::syncRate);
          Assertions.assertDoesNotThrow(m::syncTimeAvg);
          Assertions.assertDoesNotThrow(m::syncTimeMax);
          Assertions.assertDoesNotThrow(m::syncTotal);
          Assertions.assertDoesNotThrow(m::assignedConnectors);
          Assertions.assertDoesNotThrow(m::assignedTasks);
        });

    var m2 = ConnectorMetrics.of(MBeanClient.local());
    Assertions.assertNotEquals(0, m2.size());
    m2.forEach(
        m -> {
          Assertions.assertDoesNotThrow(m::connectionCloseRate);
          Assertions.assertDoesNotThrow(m::connectionCloseTotal);
          Assertions.assertDoesNotThrow(m::connectionCount);
          Assertions.assertDoesNotThrow(m::connectionCreationRate);
          Assertions.assertDoesNotThrow(m::connectionCreationTotal);
          Assertions.assertDoesNotThrow(m::failedAuthenticationRate);
          Assertions.assertDoesNotThrow(m::failedAuthenticationTotal);
          Assertions.assertDoesNotThrow(m::failedReauthenticationRate);
          Assertions.assertDoesNotThrow(m::failedReauthenticationTotal);
          Assertions.assertDoesNotThrow(m::incomingByteRate);
          Assertions.assertDoesNotThrow(m::incomingByteTotal);
          Assertions.assertDoesNotThrow(m::ioRatio);
          Assertions.assertDoesNotThrow(m::ioTimeNsAvg);
          Assertions.assertDoesNotThrow(m::ioTimeNsTotal);
          Assertions.assertDoesNotThrow(m::ioWaitRatio);
          Assertions.assertDoesNotThrow(m::ioWaitTimeNsAvg);
          Assertions.assertDoesNotThrow(m::ioWaitTimeNsTotal);
          Assertions.assertDoesNotThrow(m::ioWaitTimeTotal);
          Assertions.assertDoesNotThrow(m::ioTimeTotal);
          Assertions.assertDoesNotThrow(m::networkIoRate);
          Assertions.assertDoesNotThrow(m::networkIoTotal);
          Assertions.assertDoesNotThrow(m::outgoingByteRate);
          Assertions.assertDoesNotThrow(m::outgoingByteTotal);
          Assertions.assertDoesNotThrow(m::reauthenticationLatencyAvg);
          Assertions.assertDoesNotThrow(m::reauthenticationLatencyMax);
          Assertions.assertDoesNotThrow(m::requestRate);
          Assertions.assertDoesNotThrow(m::requestSizeAvg);
          Assertions.assertDoesNotThrow(m::requestSizeMax);
          Assertions.assertDoesNotThrow(m::requestTotal);
          Assertions.assertDoesNotThrow(m::selectRate);
          Assertions.assertDoesNotThrow(m::selectTotal);
          Assertions.assertDoesNotThrow(m::successfulAuthenticationNoReauthTotal);
          Assertions.assertDoesNotThrow(m::successfulAuthenticationRate);
          Assertions.assertDoesNotThrow(m::successfulAuthenticationTotal);
          Assertions.assertDoesNotThrow(m::successfulReauthenticationRate);
          Assertions.assertDoesNotThrow(m::successfulReauthenticationTotal);
        });

    var m3 = ConnectorMetrics.nodeInfo(MBeanClient.local());
    Assertions.assertNotEquals(0, m3.size());
    m3.forEach(
        m -> {
          Assertions.assertDoesNotThrow(m::incomingByteRate);
          Assertions.assertDoesNotThrow(m::incomingByteTotal);
          Assertions.assertDoesNotThrow(m::outgoingByteRate);
          Assertions.assertDoesNotThrow(m::outgoingByteTotal);
          Assertions.assertDoesNotThrow(m::requestLatencyAvg);
          Assertions.assertDoesNotThrow(m::requestLatencyMax);
          Assertions.assertDoesNotThrow(m::requestRate);
          Assertions.assertDoesNotThrow(m::requestSizeAvg);
          Assertions.assertDoesNotThrow(m::requestSizeMax);
          Assertions.assertDoesNotThrow(m::requestTotal);
          Assertions.assertDoesNotThrow(m::responseRate);
          Assertions.assertDoesNotThrow(m::responseTotal);
        });

    var m4 = ConnectorMetrics.workerInfo(MBeanClient.local());
    Assertions.assertNotEquals(0, m4.size());
    m4.forEach(
        m -> {
          Assertions.assertDoesNotThrow(m::connectorCount);
          Assertions.assertDoesNotThrow(m::connectorStartupAttemptsTotal);
          Assertions.assertDoesNotThrow(m::connectorStartupFailurePercentage);
          Assertions.assertDoesNotThrow(m::connectorStartupFailureTotal);
          Assertions.assertDoesNotThrow(m::connectorStartupSuccessPercentage);
          Assertions.assertDoesNotThrow(m::connectorStartupSuccessTotal);
          Assertions.assertDoesNotThrow(m::taskCount);
          Assertions.assertDoesNotThrow(m::taskStartupAttemptsTotal);
          Assertions.assertDoesNotThrow(m::taskStartupFailurePercentage);
          Assertions.assertDoesNotThrow(m::taskStartupFailureTotal);
          Assertions.assertDoesNotThrow(m::taskStartupSuccessPercentage);
          Assertions.assertDoesNotThrow(m::taskStartupSuccessTotal);
        });

    var m5 = ConnectorMetrics.workerRebalanceInfo(MBeanClient.local());
    Assertions.assertNotEquals(0, m5.size());
    m5.forEach(
        m -> {
          Assertions.assertDoesNotThrow(m::completedRebalancesTotal);
          Assertions.assertDoesNotThrow(m::connectProtocol);
          Assertions.assertDoesNotThrow(m::epoch);
          Assertions.assertDoesNotThrow(m::leaderName);
          Assertions.assertDoesNotThrow(m::rebalanceAvgTimeMs);
          Assertions.assertDoesNotThrow(m::rebalanceMaxTimeMs);
          Assertions.assertDoesNotThrow(m::rebalancing);
          Assertions.assertDoesNotThrow(m::timeSinceLastRebalanceMs);
        });
  }
}
