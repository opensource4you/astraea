package org.astraea.performance.latency;

import java.time.Duration;
import java.util.concurrent.*;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class End2EndLatencyTest {
  @Test
  void testExecute() throws Exception {
    var factory = new FakeComponentFactory();
    var parameters = new End2EndLatencyArgument();
    parameters.brokers = "brokers";
    parameters.topic = "topic";
    parameters.numberOfProducers = 1;
    parameters.numberOfConsumers = 1;
    parameters.duration = Duration.ofSeconds(1);
    parameters.valueSize = 10;
    parameters.flushDuration = Duration.ofSeconds(1);
    try (var r = End2EndLatency.execute(factory, parameters)) {
      TimeUnit.SECONDS.sleep(2);
    }

    // check producers count
    Assertions.assertTrue(factory.producerSendCount.get() > 0);
    Assertions.assertTrue(factory.producerFlushCount.get() > 0);
    Assertions.assertEquals(1, factory.producerCloseCount.get());

    // check consumer count
    Assertions.assertTrue(factory.consumerPoolCount.get() > 0);
    Assertions.assertEquals(1, factory.consumerWakeupCount.get());
    Assertions.assertEquals(1, factory.consumerCloseCount.get());

    // check admin topic count
    Assertions.assertEquals(1, factory.topicAdminListCount.get());
    Assertions.assertEquals(1, factory.topicAdminCloseCount.get());
    Assertions.assertEquals(1, factory.topicAdminCreateCount.get());
  }
}
