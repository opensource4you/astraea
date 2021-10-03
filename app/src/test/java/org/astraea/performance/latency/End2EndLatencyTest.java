package org.astraea.performance.latency;

import com.beust.jcommander.ParameterException;
import java.time.Duration;
import java.util.concurrent.*;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class End2EndLatencyTest {

  @Test
  void testIncorrectParameters() {
    Assertions.assertThrows(
        ParameterException.class,
        () -> End2EndLatency.parameters(new String[] {End2EndLatencyArgument.BROKERS_KEY, ""}));
    Assertions.assertThrows(
        ParameterException.class,
        () ->
            End2EndLatency.parameters(
                new String[] {
                  End2EndLatencyArgument.BROKERS_KEY, "localhost:11111",
                  End2EndLatencyArgument.TOPIC_KEY, ""
                }));
    Assertions.assertThrows(
        ParameterException.class,
        () ->
            End2EndLatency.parameters(
                new String[] {
                  End2EndLatencyArgument.BROKERS_KEY, "localhost:11111",
                  End2EndLatencyArgument.DURATION_KEY, "-1"
                }));
    Assertions.assertThrows(
        ParameterException.class,
        () ->
            End2EndLatency.parameters(
                new String[] {
                  End2EndLatencyArgument.BROKERS_KEY, "localhost:11111",
                  End2EndLatencyArgument.FLUSH_DURATION_KEY, "-1"
                }));
    Assertions.assertThrows(
        ParameterException.class,
        () ->
            End2EndLatency.parameters(
                new String[] {
                  End2EndLatencyArgument.BROKERS_KEY, "localhost:11111",
                  End2EndLatencyArgument.PRODUCERS_KEY, "-1"
                }));
    Assertions.assertThrows(
        ParameterException.class,
        () ->
            End2EndLatency.parameters(
                new String[] {
                  End2EndLatencyArgument.BROKERS_KEY, "localhost:11111",
                  End2EndLatencyArgument.PRODUCERS_KEY, "0"
                }));
    Assertions.assertThrows(
        ParameterException.class,
        () ->
            End2EndLatency.parameters(
                new String[] {
                  End2EndLatencyArgument.BROKERS_KEY, "localhost:11111",
                  End2EndLatencyArgument.CONSUMERS_KEY, "-1"
                }));

    Assertions.assertEquals(
        0,
        End2EndLatency.parameters(
                new String[] {
                  End2EndLatencyArgument.BROKERS_KEY, "localhost:11111",
                  End2EndLatencyArgument.CONSUMERS_KEY, "0"
                })
            .numberOfConsumers);
  }

  @Test
  void testParameters() {
    var brokers = "broker00:12345";
    var topic = "topic";
    var numberOfProducers = 100;
    var numberOfConsumers = 10;
    var duration = Duration.ofSeconds(10);
    var valueSize = 888;
    var flushDuration = Duration.ofSeconds(3);
    var parameters =
        End2EndLatency.parameters(
            new String[] {
              End2EndLatencyArgument.BROKERS_KEY,
              brokers,
              End2EndLatencyArgument.CONSUMERS_KEY,
              String.valueOf(numberOfConsumers),
              End2EndLatencyArgument.DURATION_KEY,
              String.valueOf(duration.toSeconds()),
              End2EndLatencyArgument.PRODUCERS_KEY,
              String.valueOf(numberOfProducers),
              End2EndLatencyArgument.TOPIC_KEY,
              topic,
              End2EndLatencyArgument.VALUE_SIZE_KEY,
              String.valueOf(valueSize),
              End2EndLatencyArgument.FLUSH_DURATION_KEY,
              String.valueOf(flushDuration.toSeconds())
            });
    Assertions.assertEquals(brokers, parameters.brokers);
    Assertions.assertEquals(topic, parameters.topic);
    Assertions.assertEquals(numberOfConsumers, parameters.numberOfConsumers);
    Assertions.assertEquals(numberOfProducers, parameters.numberOfProducers);
    Assertions.assertEquals(duration, parameters.duration);
    Assertions.assertEquals(valueSize, parameters.valueSize);
    Assertions.assertEquals(flushDuration, parameters.flushDuration);
  }

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
