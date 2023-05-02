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
package org.astraea.common.metrics.collector;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import org.astraea.common.metrics.ClusterBean;
import org.astraea.common.metrics.HasBeanObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class MetricSensorTest {

  @Test
  void testMultipleSensors() {
    var mbean0 = Mockito.mock(HasBeanObject.class);
    MetricSensor metricSensor0 = (client, ignored) -> List.of(mbean0);
    var mbean1 = Mockito.mock(HasBeanObject.class);
    MetricSensor metricSensor1 = (client, ignored) -> List.of(mbean1);

    var sensor = MetricSensor.of(List.of(metricSensor0, metricSensor1)).get();

    var result = sensor.fetch(Mockito.mock(BeanObjectClient.class), ClusterBean.EMPTY);

    Assertions.assertEquals(2, result.size());
    Assertions.assertTrue(result.contains(mbean0));
    Assertions.assertTrue(result.contains(mbean1));
  }

  @Test
  void testEmpty() {
    Assertions.assertEquals(Optional.empty(), MetricSensor.of(List.of()));
  }

  @Test
  void testNoSwallowException() {
    var result = List.of(Mockito.mock(HasBeanObject.class));
    MetricSensor goodMetricSensor = (client, ignored) -> result;
    MetricSensor badMetricSensor =
        (client, ignored) -> {
          throw new RuntimeException("xxx");
        };
    var sensor =
        MetricSensor.of(
                List.of(badMetricSensor, goodMetricSensor),
                e -> {
                  if (e instanceof RuntimeException) {
                    throw new RuntimeException();
                  }
                })
            .get();
    Assertions.assertThrows(
        RuntimeException.class,
        () -> sensor.fetch(Mockito.mock(BeanObjectClient.class), ClusterBean.EMPTY));
  }

  @Test
  void testSensorsWithExceptionHandler() {
    var mbean0 = Mockito.mock(HasBeanObject.class);
    MetricSensor metricSensor0 = (client, ignored) -> List.of(mbean0);
    MetricSensor metricSensor1 =
        (client, ignored) -> {
          throw new NoSuchElementException();
        };
    MetricSensor metricSensor2 =
        (client, ignored) -> {
          throw new RuntimeException();
        };

    var sensor = MetricSensor.of(List.of(metricSensor0, metricSensor1)).get();
    Assertions.assertDoesNotThrow(
        () -> sensor.fetch(Mockito.mock(BeanObjectClient.class), ClusterBean.EMPTY));
    Assertions.assertEquals(
        1, sensor.fetch(Mockito.mock(BeanObjectClient.class), ClusterBean.EMPTY).size());

    Assertions.assertDoesNotThrow(
        () ->
            MetricSensor.of(List.of(metricSensor0, metricSensor2))
                .get()
                .fetch(Mockito.mock(BeanObjectClient.class), ClusterBean.EMPTY));
    Assertions.assertThrows(
        NoSuchElementException.class,
        () ->
            MetricSensor.of(
                    List.of(metricSensor1, metricSensor2),
                    e -> {
                      if (e instanceof NoSuchElementException) throw new NoSuchElementException();
                    })
                .get()
                .fetch(Mockito.mock(BeanObjectClient.class), ClusterBean.EMPTY));
  }
}
