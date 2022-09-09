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

import java.time.Duration;
import java.util.List;
import org.astraea.common.Utils;
import org.astraea.common.metrics.client.HasNodeMetrics;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TrackerTest {

  @Test
  void testClose() {
    var tracker = TrackerThread.create(() -> List.of(), () -> List.of(), ExeTime.of("1records"));
    Assertions.assertFalse(tracker.closed());
    tracker.close();
    Assertions.assertTrue(tracker.closed());
  }

  @Test
  void testZeroConsumer() {
    var producerReport = new ProducerThread.Report();
    var tracker =
        TrackerThread.create(
            () -> List.of(producerReport), () -> List.of(), ExeTime.of("1records"));
    Assertions.assertFalse(tracker.closed());
    producerReport.record("topic", 1, 100, 1L, 1);
    // wait to done
    Utils.sleep(Duration.ofSeconds(2));
    Assertions.assertTrue(tracker.closed());
  }

  @Test
  void testExeTime() {
    var producerReport = new ProducerThread.Report();
    var consumerReport = new ConsumerThread.Report("xxx");
    var tracker =
        TrackerThread.create(
            () -> List.of(producerReport), () -> List.of(consumerReport), ExeTime.of("2s"));
    Assertions.assertFalse(tracker.closed());
    producerReport.record("topic", 1, 100, 1L, 1);
    consumerReport.record("topic", 1, 100, 1L, 1);
    Utils.sleep(Duration.ofSeconds(3));
    Assertions.assertTrue(tracker.closed());
  }

  @Test
  void testConsumerAndProducer() {
    var producerReport = new ProducerThread.Report();
    var consumerReport = new ConsumerThread.Report("xxx");
    var tracker =
        TrackerThread.create(
            () -> List.of(producerReport), () -> List.of(consumerReport), ExeTime.of("1records"));
    Assertions.assertFalse(tracker.closed());
    producerReport.record("topic", 1, 100, 1L, 1);
    consumerReport.record("topic", 1, 100, 1L, 1);
    // wait to done
    Utils.sleep(Duration.ofSeconds(2));
    Assertions.assertTrue(tracker.closed());
  }

  @Test
  void testSumOfAttribute() {
    var hasNodeMetrics = Mockito.mock(HasNodeMetrics.class);
    var hasNodeMetrics2 = Mockito.mock(HasNodeMetrics.class);
    Mockito.when(hasNodeMetrics.incomingByteTotal()).thenReturn(2D);
    Mockito.when(hasNodeMetrics2.incomingByteTotal()).thenReturn(3D);
    Mockito.when(hasNodeMetrics.createdTimestamp()).thenReturn(System.currentTimeMillis());
    Mockito.when(hasNodeMetrics2.createdTimestamp()).thenReturn(System.currentTimeMillis());
    Assertions.assertEquals(
        5D,
        TrackerThread.sumOfAttribute(
            List.of(hasNodeMetrics, hasNodeMetrics2), HasNodeMetrics::incomingByteTotal));
  }
}
