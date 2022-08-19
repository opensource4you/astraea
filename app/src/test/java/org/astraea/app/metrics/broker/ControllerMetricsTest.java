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

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;
import java.util.Locale;
import org.astraea.app.metrics.MBeanClient;
import org.astraea.app.metrics.MetricsTestUtil;
import org.astraea.app.service.RequireSingleBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class ControllerMetricsTest extends RequireSingleBrokerCluster {

  @ParameterizedTest
  @EnumSource(ControllerMetrics.Controller.class)
  void testController(ControllerMetrics.Controller controller) {
    var gauge = controller.fetch(MBeanClient.local());
    MetricsTestUtil.validate(gauge);
    Assertions.assertEquals(controller, gauge.type());
  }

  @ParameterizedTest
  @EnumSource(ControllerMetrics.ControllerState.class)
  void testControllerState(ControllerMetrics.ControllerState controllerState) {
    var timer = controllerState.fetch(MBeanClient.local());
    MetricsTestUtil.validate(timer);
  }

  @Test
  void testControllerStateNonEnum() {
    var meter =
        ControllerMetrics.ControllerState.getUncleanLeaderElectionsPerSec(MBeanClient.local());
    MetricsTestUtil.validate(meter);
  }

  @Test
  void testKafkaMetricsOf() {
    Arrays.stream(ControllerMetrics.Controller.values())
        .forEach(
            t -> {
              Assertions.assertEquals(
                  t, ControllerMetrics.Controller.of(t.metricName().toLowerCase(Locale.ROOT)));
              Assertions.assertEquals(
                  t, ControllerMetrics.Controller.of(t.metricName().toUpperCase(Locale.ROOT)));
            });

    assertThrows(IllegalArgumentException.class, () -> ControllerMetrics.Controller.of("nothing"));
  }

  @Test
  void testAllEnumNameUnique() {
    Assertions.assertTrue(
        MetricsTestUtil.metricDistinct(
            ControllerMetrics.Controller.values(), ControllerMetrics.Controller::metricName));

    Assertions.assertTrue(
        MetricsTestUtil.metricDistinct(
            ControllerMetrics.ControllerState.values(),
            ControllerMetrics.ControllerState::metricName));
  }
}
