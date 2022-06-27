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
package org.astraea.app.performance;

import java.util.Random;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class MetricsTest {

  @Test
  void testAverage() {
    Random rand = new Random();
    final int num = 1000;
    double avg = 0.0;
    Metrics metrics = new Metrics();

    Assertions.assertEquals(0, metrics.avgLatency());

    for (int i = 0; i < num; ++i) {
      long next = rand.nextInt();
      avg += ((double) next - avg) / (i + 1);
      metrics.accept(next, 0);
    }

    Assertions.assertEquals(avg, metrics.avgLatency());
  }

  @Test
  void testBytes() {
    var metrics = new Metrics();

    Assertions.assertEquals(0, metrics.bytes());
    metrics.accept(0L, 1000);
    Assertions.assertEquals(1000, metrics.bytes());
  }

  @Test
  void testCurrentBytes() {
    var metrics = new Metrics();

    Assertions.assertEquals(0, metrics.clearAndGetCurrentBytes());
    metrics.accept(0L, 100);
    metrics.accept(0L, 101);
    Assertions.assertEquals(201, metrics.clearAndGetCurrentBytes());
    Assertions.assertEquals(0, metrics.clearAndGetCurrentBytes());
  }
}
