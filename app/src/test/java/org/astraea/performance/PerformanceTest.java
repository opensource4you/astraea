package org.astraea.performance;

import java.util.concurrent.CountDownLatch;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class PerformanceTest {
  @Test
  void testParseParameter() {
    Performance.Parameters param =
        Performance.Parameters.parseArgs(
            new String[] {
              "--brokers",
              "localhost:9092",
              "--topic",
              "testing",
              "--topicConfigs",
              "partitions:3,replicationFactor:3",
              "--producers",
              "6",
              "--consumers",
              "3",
              "--records",
              "10000",
              "--recordSize",
              "1000000"
            });

    Assertions.assertEquals("localhost:9092", param.brokers);
    Assertions.assertEquals("testing", param.topic);
    Assertions.assertEquals("partitions:3,replicationFactor:3", param.topicConfigs);
    Assertions.assertEquals(6, param.producers);
    Assertions.assertEquals(3, param.consumers);
    Assertions.assertEquals(10000, param.records);
    Assertions.assertEquals(1000000, param.recordSize);

    Assertions.assertThrows(
        IllegalArgumentException.class, () -> Performance.Parameters.parseArgs(new String[] {}));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> Performance.Parameters.parseArgs(new String[] {"--brokers", "localhost:9092"}));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            Performance.Parameters.parseArgs(
                new String[] {
                  "--brokers", "localhost:9092", "--topic", "testing", "--producers", "0"
                }));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            Performance.Parameters.parseArgs(
                new String[] {
                  "--brokers", "localhost:9092", "--topic", "testing", "--consumers", "-1"
                }));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            Performance.Parameters.parseArgs(
                new String[] {
                  "--brokers", "localhost:9092", "--topic", "testing", "--records", "0"
                }));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            Performance.Parameters.parseArgs(
                new String[] {
                  "--brokers", "localhost:9092", "--topic", "testing", "--recordSize", "0"
                }));
  }

  @Test
  void testCheckTopic() {
    FakeComponentFactory componentFactory = new FakeComponentFactory();

    Assertions.assertNull(componentFactory.topicToConfig.get("topic"));

    // act
    Performance.checkTopic(componentFactory, "topic", "partitions:6,replicationFactor:3");

    // assert
    Assertions.assertNotNull(componentFactory.topicToConfig.get("topic"));
    Assertions.assertEquals(6, componentFactory.topicToConfig.get("topic").numPartitions());
    Assertions.assertEquals(3, componentFactory.topicToConfig.get("topic").replicationFactor());

    // act
    Performance.checkTopic(componentFactory, "topic", "partitions:16,replicationFactor:1");

    // assert
    Assertions.assertNotNull(componentFactory.topicToConfig.get("topic"));
    Assertions.assertEquals(6, componentFactory.topicToConfig.get("topic").numPartitions());
    Assertions.assertEquals(3, componentFactory.topicToConfig.get("topic").replicationFactor());
  }

  @Test
  public void testStartProducer() throws InterruptedException {
    FakeComponentFactory componentFactory = new FakeComponentFactory();

    Metrics[] metrics =
        Performance.startProducers(
            componentFactory,
            new Performance.Parameters("localhost:9092", "topic", "", 2, 2, 4, 1));
    Thread.sleep(10);

    Assertions.assertEquals(4, componentFactory.produced.sum());
    Assertions.assertEquals(2, componentFactory.producerClosed.get());
    Assertions.assertEquals(2, metrics[0].num());
    Assertions.assertEquals(2, metrics[1].num());
  }

  @Test
  public void testStartConsumerAndStop() throws InterruptedException {
    FakeComponentFactory componentFactory = new FakeComponentFactory();
    CountDownLatch consumerComplete = new CountDownLatch(1);

    Metrics[] metrics =
        Performance.startConsumers(
            componentFactory,
            new Performance.Parameters("localhost:9092", "topic", "", 2, 2, 4, 1),
            consumerComplete);
    Thread.sleep(10);
    consumerComplete.countDown();
    Thread.sleep(10);

    Assertions.assertEquals(2, componentFactory.consumerClosed.get());
  }
}
