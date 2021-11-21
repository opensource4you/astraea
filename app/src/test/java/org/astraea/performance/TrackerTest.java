package org.astraea.performance;

import java.time.Duration;
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
    var oneRecord = new Manager(1, Duration.ofSeconds(100), true, 1024, 1);

    try (Tracker tracker = new Tracker(producerData, consumerData, oneRecord)) {
      Assertions.assertEquals(ThreadPool.Executor.State.RUNNING, tracker.execute());
      // Mock record producing
      oneRecord.payload();
      producerData.get(0).put(1, 1);
      oneRecord.consumedIncrement();
      consumerData.get(0).put(1, 1);
      Assertions.assertEquals(ThreadPool.Executor.State.DONE, tracker.execute());
    }

    // Zero consumer
    producerData = List.of(new Metrics());
    try (Tracker tracker = new Tracker(producerData, empty, oneRecord)) {
      Assertions.assertEquals(ThreadPool.Executor.State.RUNNING, tracker.execute());
      producerData.get(0).put(1, 1);
      Assertions.assertEquals(ThreadPool.Executor.State.DONE, tracker.execute());
    }

    // Stop by duration time out
    var twoSecond = new Manager(Long.MAX_VALUE, Duration.ofSeconds(2), true, 1024, 1);
    producerData = List.of(new Metrics());
    consumerData = List.of(new Metrics());
    try (Tracker tracker = new Tracker(producerData, consumerData, twoSecond)) {
      tracker.start = System.currentTimeMillis();
      Assertions.assertEquals(ThreadPool.Executor.State.RUNNING, tracker.execute());

      // Mock record producing
      twoSecond.payload();
      producerData.get(0).put(1, 1);
      twoSecond.consumedIncrement();
      consumerData.get(0).put(1, 1);
      Thread.sleep(2000);

      Assertions.assertEquals(ThreadPool.Executor.State.DONE, tracker.execute());
    }
  }
}
