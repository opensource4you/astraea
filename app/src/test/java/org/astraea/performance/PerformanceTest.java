package org.astraea.performance;

import java.util.concurrent.CountDownLatch;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

package org.astraea.performance;

import java.util.List;
import org.astraea.concurrent.ThreadPool;
import org.astraea.service.RequireBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class PerformanceTest extends RequireBrokerCluster {
  private final FakeComponentFactory fakeFactory = new FakeComponentFactory();
  private final ComponentFactory factory = ComponentFactory.fromKafka(bootstrapServers());

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
            Performance.producerExecutor(
                    fakeFactory.createProducer(), new Performance.Argument(), metrics);

    executor.execute();

    Assertions.assertEquals(1, fakeFactory.produced.intValue());
    Assertions.assertEquals(1, metrics.num());
    Assertions.assertEquals(1024, metrics.bytesThenReset());

    executor.cleanup();

    Assertions.assertEquals(1, fakeFactory.producerClosed.get());
  }

  @Test
  public void testConsumerExecutor() throws InterruptedException {
    Metrics metrics = new Metrics();
    ThreadPool.Executor executor =
            Performance.consumerExecutor(fakeFactory.createConsumer(List.of("topic")), metrics);

    executor.execute();

    Assertions.assertEquals(1, fakeFactory.consumerPoll.get());
    Assertions.assertEquals(1, metrics.num());
    Assertions.assertEquals(1024, metrics.bytesThenReset());

    executor.cleanup();

    Assertions.assertEquals(1, fakeFactory.consumerClosed.get());
  }
}