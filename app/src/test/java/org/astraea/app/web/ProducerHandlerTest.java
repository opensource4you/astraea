package org.astraea.app.web;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.astraea.app.admin.Admin;
import org.astraea.app.common.Utils;
import org.astraea.app.producer.Producer;
import org.astraea.app.service.RequireBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ProducerHandlerTest extends RequireBrokerCluster {

  @Test
  void testListProducers() throws ExecutionException, InterruptedException {
    var topicName = Utils.randomString(10);
    try (var admin = Admin.of(bootstrapServers());
        var producer = Producer.of(bootstrapServers())) {
      var handler = new ProducerHandler(admin);
      producer.sender().topic(topicName).value(new byte[1]).run().toCompletableFuture().get();

      var result =
          Assertions.assertInstanceOf(
              ProducerHandler.Partitions.class, handler.get(Optional.empty(), Map.of()));
      Assertions.assertNotEquals(0, result.partitions.size());

      var partitions =
          result.partitions.stream()
              .filter(t -> t.topic.equals(topicName))
              .collect(Collectors.toUnmodifiableList());
      Assertions.assertEquals(1, partitions.size());
      Assertions.assertEquals(topicName, partitions.iterator().next().topic);
      Assertions.assertEquals(0, partitions.iterator().next().partition);
    }
  }

  @Test
  void testQuerySinglePartition() throws ExecutionException, InterruptedException {
    var topicName = Utils.randomString(10);
    try (var admin = Admin.of(bootstrapServers());
        var producer = Producer.of(bootstrapServers())) {
      admin.creator().topic(topicName).numberOfPartitions(2).create();
      TimeUnit.SECONDS.sleep(2);
      var handler = new ProducerHandler(admin);
      producer
          .sender()
          .topic(topicName)
          .partition(0)
          .value(new byte[1])
          .run()
          .toCompletableFuture()
          .get();
      producer
          .sender()
          .topic(topicName)
          .partition(1)
          .value(new byte[1])
          .run()
          .toCompletableFuture()
          .get();
      TimeUnit.SECONDS.sleep(2);

      Assertions.assertEquals(
          1,
          handler
              .partitions(
                  Map.of(ProducerHandler.TOPIC_KEY, topicName, ProducerHandler.PARTITION_KEY, "0"))
              .size());

      var result0 =
          Assertions.assertInstanceOf(
              ProducerHandler.Partitions.class,
              handler.get(
                  Optional.empty(),
                  Map.of(
                      ProducerHandler.TOPIC_KEY, topicName, ProducerHandler.PARTITION_KEY, "0")));
      Assertions.assertEquals(1, result0.partitions.size());
      Assertions.assertEquals(topicName, result0.partitions.iterator().next().topic);
      Assertions.assertEquals(0, result0.partitions.iterator().next().partition);

      Assertions.assertEquals(
          2, handler.partitions(Map.of(ProducerHandler.TOPIC_KEY, topicName)).size());

      var result1 =
          Assertions.assertInstanceOf(
              ProducerHandler.Partitions.class,
              handler.get(Optional.empty(), Map.of(ProducerHandler.TOPIC_KEY, topicName)));
      Assertions.assertEquals(2, result1.partitions.size());
      Assertions.assertEquals(topicName, result1.partitions.iterator().next().topic);
      Assertions.assertEquals(
          Set.of(0, 1),
          result1.partitions.stream().map(p -> p.partition).collect(Collectors.toSet()));
    }
  }

  @Test
  void testPartitions() throws InterruptedException {
    var topicName = Utils.randomString(10);
    try (var admin = Admin.of(bootstrapServers())) {
      var handler = new ProducerHandler(admin);
      Assertions.assertEquals(admin.partitions(), handler.partitions(Map.of()));
      var target = admin.partitions().iterator().next();
      Assertions.assertEquals(
          Set.of(target),
          handler.partitions(
              Map.of(
                  ProducerHandler.TOPIC_KEY,
                  target.topic(),
                  ProducerHandler.PARTITION_KEY,
                  String.valueOf(target.partition()))));

      Assertions.assertThrows(
          IllegalArgumentException.class,
          () ->
              handler.partitions(
                  Map.of(
                      ProducerHandler.TOPIC_KEY,
                      target.topic(),
                      ProducerHandler.PARTITION_KEY,
                      "a")));

      admin.creator().topic(topicName).numberOfPartitions(3).create();
      TimeUnit.SECONDS.sleep(2);

      Assertions.assertEquals(
          3, handler.partitions(Map.of(ProducerHandler.TOPIC_KEY, topicName)).size());
    }
  }
}
