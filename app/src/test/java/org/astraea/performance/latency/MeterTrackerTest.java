package org.astraea.performance.latency;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class MeterTrackerTest {

  @Test
  void testRecord() {
    var tracker = new MeterTracker("test tracker");
    Assertions.assertEquals(0, tracker.count());
    Assertions.assertEquals(0, tracker.maxLatency());
    Assertions.assertEquals(0, tracker.averageLatency());

    tracker.record(10, 10);
    Assertions.assertEquals(1, tracker.count());
    Assertions.assertEquals(10, tracker.bytes());
    Assertions.assertEquals(10, tracker.maxLatency());
    Assertions.assertEquals(10, tracker.averageLatency());
  }
}
