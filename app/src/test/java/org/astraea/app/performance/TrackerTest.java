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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import org.astraea.common.Utils;
import org.astraea.common.metrics.client.HasNodeMetrics;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TrackerTest {

  @Test
  void testEmptyReports() {
    var tracker = TrackerThread.create(List::of, List::of);
    Utils.sleep(Duration.ofSeconds(2));
    Assertions.assertTrue(tracker.closed());
  }

  @Test
  void testClose() {
    var closed = new AtomicBoolean(false);
    var tracker = TrackerThread.create(List::of, () -> List.of(Report.of("c", closed::get)));
    Assertions.assertFalse(tracker.closed());
    closed.set(true);
    Utils.sleep(Duration.ofSeconds(2));
    Assertions.assertTrue(tracker.closed());
  }

  @Test
  void testPartialClose() {
    var closed0 = new AtomicBoolean(false);
    var closed1 = new AtomicBoolean(false);
    var tracker =
        TrackerThread.create(
            () -> List.of(Report.of("c", closed0::get)),
            () -> List.of(Report.of("c", closed1::get)));
    Assertions.assertFalse(tracker.closed());
    closed0.set(true);
    Utils.sleep(Duration.ofSeconds(2));
    Assertions.assertFalse(tracker.closed());
    closed1.set(true);
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

  @Test
  void testTrackerThread() {
    var producerClosed = new AtomicBoolean(false);
    var consumerClosed = new AtomicBoolean(false);
    var closed = new AtomicBoolean(false);
    var f =
        CompletableFuture.runAsync(
            TrackerThread.trackerLoop(
                closed::get,
                () -> List.of(Report.of("c", producerClosed::get)),
                () -> List.of(Report.of("c", consumerClosed::get))));
    Assertions.assertFalse(f.isDone());
    producerClosed.set(true);
    Utils.sleep(Duration.ofSeconds(2));
    Assertions.assertFalse(f.isDone());
    consumerClosed.set(true);
    Utils.sleep(Duration.ofSeconds(2));
    Assertions.assertTrue(f.isDone());
  }

  @Test
  void testProducerPrinter() {
    var report = Report.of("c", () -> false);
    var printer = new TrackerThread.ProducerPrinter(() -> List.of(report));
    Assertions.assertFalse(printer.tryToPrint(Duration.ofSeconds(1)));
    report.record(10, 100);
    Assertions.assertTrue(printer.tryToPrint(Duration.ofSeconds(1)));
  }

  @Test
  void testConsumerPrinter() {
    var report = Report.of("c", () -> false);
    var printer = new TrackerThread.ConsumerPrinter(() -> List.of(report));
    Assertions.assertFalse(printer.tryToPrint(Duration.ofSeconds(1)));
    report.record(10, 100);
    Assertions.assertTrue(printer.tryToPrint(Duration.ofSeconds(1)));
  }
}
