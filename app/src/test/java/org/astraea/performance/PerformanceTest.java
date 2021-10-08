package org.astraea.performance;

import java.util.concurrent.CountDownLatch;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class PerformanceTest {
  @Test
  public void testCheckTopic() {
    FakeComponentFactory componentFactory = new FakeComponentFactory();
    PerformanceArgument param = new PerformanceArgument();
    param.topic = "topic";
    param.partitions = 6;
    param.replicationFactor = 3;

    Assertions.assertNull(componentFactory.topicToConfig.get("topic"));

    // act
    Performance.checkTopic(componentFactory, param);

    // assert
    Assertions.assertNotNull(componentFactory.topicToConfig.get("topic"));
    Assertions.assertEquals(6, componentFactory.topicToConfig.get("topic").numPartitions());
    Assertions.assertEquals(3, componentFactory.topicToConfig.get("topic").replicationFactor());

    param.topic = "topic";
    param.partitions = 16;
    param.replicationFactor = 1;
    // act
    Performance.checkTopic(componentFactory, param);

    // assert
    Assertions.assertNotNull(componentFactory.topicToConfig.get("topic"));
    Assertions.assertEquals(6, componentFactory.topicToConfig.get("topic").numPartitions());
    Assertions.assertEquals(3, componentFactory.topicToConfig.get("topic").replicationFactor());
  }

  @Test
  public void testStartProducer() throws InterruptedException {
    FakeComponentFactory componentFactory = new FakeComponentFactory();
    PerformanceArgument param = new PerformanceArgument();
    param.brokers = "localhost:9092";
    param.topic = "topic";
    param.producers = 2;
    param.consumers = 3;
    param.records = 4;

    Metrics[] metrics = Performance.startProducers(componentFactory, param);
    Thread.sleep(10);

    Assertions.assertEquals(4, componentFactory.produced.sum());
    Assertions.assertEquals(2, componentFactory.producerClosed.get());
    Assertions.assertEquals(2, metrics[0].num());
    Assertions.assertEquals(2, metrics[1].num());
  }

  @Test
  public void testStartConsumerAndStop() throws InterruptedException {
    var componentFactory = new FakeComponentFactory();
    var consumerComplete = new CountDownLatch(1);
    var param = new PerformanceArgument();
    param.brokers = "localhost:9092";
    param.topic = "topic";
    param.consumers = 2;

    Metrics[] metrics = Performance.startConsumers(componentFactory, param, consumerComplete);
    Thread.sleep(10);
    consumerComplete.countDown();
    Thread.sleep(10);

    Assertions.assertEquals(2, componentFactory.consumerClosed.get());
  }
}
