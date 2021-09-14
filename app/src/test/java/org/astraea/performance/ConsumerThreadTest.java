package org.astraea.performance;

import java.util.Collections;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ConsumerThreadTest {
  @Test
  void testExecuteAndClose() throws InterruptedException {
    FakeComponentFactory componentFactory = new FakeComponentFactory();
    Metrics metrics = new Metrics();

    ConsumerThread thread =
        new ConsumerThread(componentFactory.createConsumer(Collections.singleton("")), metrics);

    Assertions.assertEquals(0, metrics.avgLatency());
    Assertions.assertEquals(0, metrics.bytes());
    Assertions.assertFalse(componentFactory.consumerClosed.get());

    thread.start();
    Thread.sleep(1);
    thread.close();

    Assertions.assertNotEquals(0, metrics.avgLatency());
    Assertions.assertNotEquals(0, metrics.bytes());
    Assertions.assertTrue(componentFactory.consumerClosed.get());
  }
}
