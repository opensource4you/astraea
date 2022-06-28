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

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.astraea.app.concurrent.State;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TrackerTest {
  @Test
  public void testTerminate() throws InterruptedException {
    var producerData = List.of(new Metrics());
    var consumerData = List.of(new Metrics());
    List<Metrics> empty = List.of();
    var argument = new Performance.Argument();
    argument.exeTime = ExeTime.of("1records");

    var manager = new Manager(argument, producerData, consumerData);
    var producerDone = new AtomicBoolean(false);
    try (Tracker tracker = new Tracker(producerData, consumerData, manager, producerDone::get)) {
      producerDone.set(false);
      Assertions.assertEquals(State.RUNNING, tracker.execute());
      producerData.get(0).accept(1L, 1);
      consumerData.get(0).accept(1L, 1);
      producerDone.set(true);
      Assertions.assertEquals(State.DONE, tracker.execute());
    }

    // Zero consumer
    producerData = List.of(new Metrics());
    manager = new Manager(argument, producerData, empty);
    try (Tracker tracker = new Tracker(producerData, empty, manager, producerDone::get)) {
      producerDone.set(false);
      Assertions.assertEquals(State.RUNNING, tracker.execute());
      producerData.get(0).accept(1L, 1);
      producerDone.set(true);
      Assertions.assertEquals(State.DONE, tracker.execute());
    }

    // Stop by duration time out
    argument.exeTime = ExeTime.of("2s");
    producerData = List.of(new Metrics());
    consumerData = List.of(new Metrics());
    manager = new Manager(argument, producerData, consumerData);
    try (Tracker tracker = new Tracker(producerData, consumerData, manager, producerDone::get)) {
      producerDone.set(false);
      tracker.start = System.currentTimeMillis();
      Assertions.assertEquals(State.RUNNING, tracker.execute());

      // Mock record producing
      producerData.get(0).accept(1L, 1);
      consumerData.get(0).accept(1L, 1);
      Thread.sleep(2000);
      producerDone.set(true);
      Assertions.assertEquals(State.DONE, tracker.execute());
    }
  }
}
