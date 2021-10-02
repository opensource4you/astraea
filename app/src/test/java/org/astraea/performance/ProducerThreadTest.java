package org.astraea.performance;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ProducerThreadTest {
  @Test
  void testExecute() throws InterruptedException {
    FakeComponentFactory componentFactory = new FakeComponentFactory();
    Metrics metrics = new Metrics();
    ProducerThread thread =
        new ProducerThread(componentFactory.createProducer(), "", 1, 10, metrics);

    Assertions.assertEquals(0.0, metrics.avgLatency());
    Assertions.assertEquals(0, metrics.bytesThenReset());
    Assertions.assertFalse(thread.closed.get());

    thread.start();
    Thread.sleep(1);
    thread.close();

    Assertions.assertNotEquals(0.0, metrics.avgLatency());
    Assertions.assertNotEquals(0, metrics.bytesThenReset());
    Assertions.assertTrue(thread.closed.get());
  }
}
