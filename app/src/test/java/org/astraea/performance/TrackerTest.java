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
    int records = 1;

    try (Tracker tracker =
        new Tracker(producerData, consumerData, records, Duration.ofSeconds(Integer.MAX_VALUE))) {
      Assertions.assertEquals(ThreadPool.Executor.State.RUNNING, tracker.execute());
      producerData.get(0).put(1, 1);
      consumerData.get(0).put(1, 1);
      Assertions.assertEquals(ThreadPool.Executor.State.DONE, tracker.execute());
    }

    // Zero consumer
    producerData = List.of(new Metrics());
    try (Tracker tracker =
        new Tracker(producerData, empty, records, Duration.ofSeconds(Integer.MAX_VALUE))) {
      Assertions.assertEquals(ThreadPool.Executor.State.RUNNING, tracker.execute());
      producerData.get(0).put(1, 1);
      Assertions.assertEquals(ThreadPool.Executor.State.DONE, tracker.execute());
    }

    // Stop by duration time out
    producerData = List.of(new Metrics());
    consumerData = List.of(new Metrics());
    records = Integer.MAX_VALUE;
    try (Tracker tracker =
        new Tracker(producerData, consumerData, Integer.MAX_VALUE, Duration.ofSeconds(1))) {
      Assertions.assertEquals(ThreadPool.Executor.State.RUNNING, tracker.execute());
      Thread.sleep(2000);
      Assertions.assertEquals(ThreadPool.Executor.State.DONE, tracker.execute());
    }
  }
}
