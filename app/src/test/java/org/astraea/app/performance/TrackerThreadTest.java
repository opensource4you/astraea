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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.astraea.common.Utils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TrackerThreadTest {

  @Test
  void testConsumersGetDoneFirst() {
    var producersDone = new AtomicBoolean(false);
    var producersCount = new AtomicInteger(0);
    var consumersDone = new AtomicBoolean(false);
    var consumersCount = new AtomicInteger(0);
    Function<Duration, Boolean> logProducers =
        duration -> {
          producersCount.incrementAndGet();
          return producersDone.get();
        };
    Function<Duration, Boolean> logConsumers =
        duration -> {
          consumersCount.incrementAndGet();
          return consumersDone.get();
        };

    var f =
        CompletableFuture.runAsync(
            TrackerThread.trackerLoop(
                System.currentTimeMillis(), () -> false, logProducers, logConsumers));
    Utils.sleep(Duration.ofSeconds(2));
    Assertions.assertFalse(f.isDone());
    Assertions.assertNotEquals(0, producersCount.get());
    Assertions.assertNotEquals(0, consumersCount.get());

    // producers are still running, so the returned "done" get ignored
    consumersDone.set(true);
    producersCount.set(0);
    consumersCount.set(0);
    Utils.sleep(Duration.ofSeconds(2));
    Assertions.assertFalse(f.isDone());
    Assertions.assertNotEquals(0, producersCount.get());
    Assertions.assertNotEquals(0, consumersCount.get());

    // ok, producers get done so all done
    producersDone.set(true);
    Utils.sleep(Duration.ofSeconds(2));
    Assertions.assertTrue(f.isDone());
  }

  @Test
  void testProducersGetDoneFirst() {
    var producersDone = new AtomicBoolean(false);
    var producersCount = new AtomicInteger(0);
    var consumersDone = new AtomicBoolean(false);
    var consumersCount = new AtomicInteger(0);
    Function<Duration, Boolean> logProducers =
        duration -> {
          producersCount.incrementAndGet();
          return producersDone.get();
        };
    Function<Duration, Boolean> logConsumers =
        duration -> {
          consumersCount.incrementAndGet();
          return consumersDone.get();
        };

    var f =
        CompletableFuture.runAsync(
            TrackerThread.trackerLoop(
                System.currentTimeMillis(), () -> false, logProducers, logConsumers));
    Utils.sleep(Duration.ofSeconds(2));
    Assertions.assertFalse(f.isDone());
    Assertions.assertNotEquals(0, producersCount.get());
    Assertions.assertNotEquals(0, consumersCount.get());

    // producers get done, so the producer count is not changed anymore
    producersDone.set(true);
    producersCount.set(0);
    consumersCount.set(0);
    Utils.sleep(Duration.ofSeconds(2));
    Assertions.assertFalse(f.isDone());
    // snapshot the count
    var current = producersCount.get();
    Utils.sleep(Duration.ofSeconds(2));
    Assertions.assertEquals(current, producersCount.get());
    Assertions.assertNotEquals(0, consumersCount.get());

    // ok, consumers get done so all done
    consumersDone.set(true);
    Utils.sleep(Duration.ofSeconds(2));
    Assertions.assertTrue(f.isDone());
  }

  @Test
  void testClose() {
    var closed = new AtomicBoolean(false);
    var f =
        CompletableFuture.runAsync(
            TrackerThread.trackerLoop(
                System.currentTimeMillis(), closed::get, d -> false, d -> false));
    Assertions.assertFalse(f.isDone());
    closed.set(true);
    Utils.sleep(Duration.ofSeconds(2));
    Assertions.assertTrue(f.isDone());
  }
}
