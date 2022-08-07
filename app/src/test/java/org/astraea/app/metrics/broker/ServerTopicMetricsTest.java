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

import org.astraea.app.metrics.MBeanClient;
import org.astraea.app.service.RequireSingleBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

public class ServerTopicMetricsTest extends RequireSingleBrokerCluster {

  @ParameterizedTest
  @EnumSource(value = ServerMetrics.Topic.class)
  void testRequestBrokerTopicMetrics(ServerMetrics.Topic metric) {
    // act
    var result = metric.fetch(MBeanClient.local());

    // assert access attribute will not throw casting error
    // assert attribute actually exists
    Assertions.assertDoesNotThrow(result::count);
    Assertions.assertDoesNotThrow(result::eventType);
    Assertions.assertDoesNotThrow(result::fifteenMinuteRate);
    Assertions.assertDoesNotThrow(result::fiveMinuteRate);
    Assertions.assertDoesNotThrow(result::meanRate);
    Assertions.assertDoesNotThrow(result::oneMinuteRate);
    Assertions.assertDoesNotThrow(result::rateUnit);
  }
}
