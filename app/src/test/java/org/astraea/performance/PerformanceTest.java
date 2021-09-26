package org.astraea.performance;

import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import org.apache.kafka.clients.admin.NewTopic;
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
  public void testWarmUp() {
    FakeComponentFactory componentFactory = new FakeComponentFactory();
    try (TopicAdmin admin = componentFactory.createAdmin()) {
      admin.createTopics(Collections.singletonList(new NewTopic("topic", 6, (short) 1)));
    } catch (Exception ignore) {
    }

    Performance.warmUp(componentFactory, "topic");

    Assertions.assertEquals(6, componentFactory.produced.sum());
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
  public void testStartConsumerAndConsumerComplete() throws InterruptedException {
    FakeComponentFactory componentFactory = new FakeComponentFactory();
    CountDownLatch consumerComplete = new CountDownLatch(1);

    Metrics[] metrics =
        Performance.startConsumers(
            componentFactory,
            new Performance.Parameters("localhost:9092", "topic", "", 2, 2, 4, 1),
            consumerComplete);
    Thread.sleep(10);

    // check if startConsumer successfully return
    Assertions.assertEquals(0, consumerComplete.getCount());
    Assertions.assertEquals(2, componentFactory.consumerClosed.get());
    Assertions.assertTrue(Performance.consumerComplete(metrics, 4));
  }
}
