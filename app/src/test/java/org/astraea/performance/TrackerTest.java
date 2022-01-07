package org.astraea.performance;

import java.util.List;
import org.astraea.concurrent.ThreadPool;
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
    try (Tracker tracker = new Tracker(producerData, consumerData, manager)) {
      Assertions.assertEquals(ThreadPool.Executor.State.RUNNING, tracker.execute());
      producerData.get(0).accept(1L, 1L);
      consumerData.get(0).accept(1L, 1L);
      manager.producerClosed();
      Assertions.assertEquals(ThreadPool.Executor.State.DONE, tracker.execute());
    }

    // Zero consumer
    producerData = List.of(new Metrics());
    manager = new Manager(argument, producerData, empty);
    try (Tracker tracker = new Tracker(producerData, empty, manager)) {
      Assertions.assertEquals(ThreadPool.Executor.State.RUNNING, tracker.execute());
      producerData.get(0).accept(1L, 1L);
      manager.producerClosed();
      Assertions.assertEquals(ThreadPool.Executor.State.DONE, tracker.execute());
    }

    // Stop by duration time out
    argument.exeTime = ExeTime.of("2s");
    producerData = List.of(new Metrics());
    consumerData = List.of(new Metrics());
    manager = new Manager(argument, producerData, consumerData);
    try (Tracker tracker = new Tracker(producerData, consumerData, manager)) {
      tracker.start = System.currentTimeMillis();
      Assertions.assertEquals(ThreadPool.Executor.State.RUNNING, tracker.execute());

      // Mock record producing
      producerData.get(0).accept(1L, 1L);
      consumerData.get(0).accept(1L, 1L);
      Thread.sleep(2000);
      manager.producerClosed();

      Assertions.assertEquals(ThreadPool.Executor.State.DONE, tracker.execute());
    }
  }
}
