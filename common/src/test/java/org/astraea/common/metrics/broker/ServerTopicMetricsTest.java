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

import org.astraea.common.metrics.JndiClient;
import org.astraea.common.metrics.MetricsTestUtils;
import org.astraea.it.Service;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

public class ServerTopicMetricsTest {

  private static final Service SERVICE = Service.builder().numberOfBrokers(1).build();

  @BeforeAll
  static void createBroker() {
    // call broker-related method to initialize broker cluster
    Assertions.assertNotEquals(0, SERVICE.dataFolders().size());
  }

  @AfterAll
  static void closeService() {
    SERVICE.close();
  }

  @ParameterizedTest
  @EnumSource(value = ServerMetrics.BrokerTopic.class)
  void testRequestBrokerTopicMetrics(ServerMetrics.BrokerTopic metric) {
    var meter = metric.fetch(JndiClient.local());
    MetricsTestUtils.validate(meter);
  }
}
