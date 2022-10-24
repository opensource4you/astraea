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
package org.astraea.common.metrics;

import java.util.concurrent.atomic.AtomicInteger;
import org.astraea.common.metrics.stats.Avg;
import org.astraea.common.metrics.stats.Stat;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class SensorTest {

  // Check if every `Stat#record` has been called when sensor called `Sensor#record`
  @Test
  void testRecord() {
    var counter1 = new AtomicInteger(0);
    var counter2 = new AtomicInteger(0);
    var sensor =
        new SensorBuilder<Double>()
            .addStat("t1", countRecord(counter1))
            .addStat("t2", countRecord(counter2))
            .build();

    sensor.record(10.0);
    sensor.record(2.0);
    Assertions.assertEquals(counter1.get(), 2);
    Assertions.assertEquals(counter2.get(), 2);
  }

  @SuppressWarnings("unchecked")
  @Test
  void testMeasure() {
    var stat1 = Mockito.mock(Stat.class);
    var stat2 = Mockito.mock(Stat.class);
    var sensor = new SensorBuilder<Double>().addStat("t1", stat1).addStat("t2", stat2).build();

    Mockito.when(stat1.measure()).thenReturn(1.0);
    Mockito.when(stat2.measure()).thenReturn(2.0);

    Assertions.assertEquals(1.0, sensor.measure("t1"));
    Assertions.assertEquals(2.0, sensor.measure("t2"));
  }

  @Test
  void testMetrics() {
    var sensor = new SensorBuilder<Double>().addStat("average", new Avg()).build();
    sensor.record(1.0);
    var metrics = sensor.metrics();
    Assertions.assertEquals(1.0, metrics.get("average").measure());
    sensor.record(2.0);
    Assertions.assertEquals(1.0, metrics.get("average").measure());
  }

  private Stat<Double> countRecord(AtomicInteger counter) {
    return new Stat<>() {
      @Override
      public void record(Double value) {
        counter.incrementAndGet();
      }

      @Override
      public Double measure() {
        return 0.0;
      }
    };
  }
}
