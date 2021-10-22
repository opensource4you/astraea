package org.astraea.performance;

import org.astraea.concurrent.ThreadPool;
import org.astraea.service.RequireBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class PerformanceTest extends RequireBrokerCluster {
  private final ComponentFactory factory =
      ComponentFactory.fromKafka(bootstrapServers(), "testing-" + System.currentTimeMillis());

  @Test
  public void testExecute() {
    Performance.Argument param = new Performance.Argument();
    param.brokers = bootstrapServers();
    Assertions.assertDoesNotThrow(() -> Performance.execute(param, factory));
  }

  @Test
  public void testProducerExecutor() throws InterruptedException {
    Metrics metrics = new Metrics();
    ThreadPool.Executor executor =
        Performance.producerExecutor(factory.createProducer(), new Performance.Argument(), metrics);

    executor.execute();

    Assertions.assertEquals(1, metrics.num());
    Assertions.assertEquals(1024, metrics.bytesThenReset());

    executor.cleanup();
  }

  @Test
  public void testConsumerExecutor() throws InterruptedException {
    Metrics metrics = new Metrics();
    ThreadPool.Executor executor = Performance.consumerExecutor(factory.createConsumer(), metrics);

    executor.execute();

    Assertions.assertEquals(0, metrics.num());
    Assertions.assertEquals(0, metrics.bytesThenReset());

    factory.createProducer().send(new byte[1024]);
    executor.execute();

    Assertions.assertEquals(1, metrics.num());
    Assertions.assertNotEquals(1024, metrics.bytesThenReset());
  }
}
