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
    var oneRecord = new DataManager(1, true, 1024);
    var maxRecord = new DataManager(Long.MAX_VALUE, true, 1024);

    try (Tracker tracker =
        new Tracker(producerData, consumerData, oneRecord, Duration.ofSeconds(Integer.MAX_VALUE))) {
      Assertions.assertEquals(ThreadPool.Executor.State.RUNNING, tracker.execute());
      // Mock record producing
      oneRecord.payload();
      producerData.get(0).put(1, 1);
      consumerData.get(0).put(1, 1);
      Assertions.assertEquals(ThreadPool.Executor.State.DONE, tracker.execute());
    }

    // Zero consumer
    producerData = List.of(new Metrics());
    try (Tracker tracker =
        new Tracker(producerData, empty, oneRecord, Duration.ofSeconds(Integer.MAX_VALUE))) {
      Assertions.assertEquals(ThreadPool.Executor.State.RUNNING, tracker.execute());
      producerData.get(0).put(1, 1);
      Assertions.assertEquals(ThreadPool.Executor.State.DONE, tracker.execute());
    }

    // Stop by duration time out
    producerData = List.of(new Metrics());
    consumerData = List.of(new Metrics());
    try (Tracker tracker =
        new Tracker(producerData, consumerData, maxRecord, Duration.ofSeconds(1))) {
      Assertions.assertEquals(ThreadPool.Executor.State.RUNNING, tracker.execute());

      // Mock record producing
      maxRecord.payload();
      producerData.get(0).put(1, 1);
      consumerData.get(0).put(1, 1);
      Thread.sleep(2000);

      Assertions.assertEquals(ThreadPool.Executor.State.DONE, tracker.execute());
    }
  }
}
