package org.astraea.web;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.astraea.admin.Admin;
import org.astraea.common.Utils;
import org.astraea.producer.Producer;
import org.astraea.service.RequireBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class PipelineHandlerTest extends RequireBrokerCluster {

  @Test
  void testQueries() {
    // this topic-partition has no producers/consumers
    var tp = new PipelineHandler.TopicPartition("g", 1);
    Assertions.assertTrue(
        PipelineHandler.filter(Map.of(PipelineHandler.ACTIVE_KEY, "false")).test(tp));
    Assertions.assertFalse(
        PipelineHandler.filter(Map.of(PipelineHandler.ACTIVE_KEY, "true")).test(tp));
    Assertions.assertTrue(PipelineHandler.filter(Map.of()).test(tp));

    // add some producers/consumers
    tp.from.add(Mockito.mock(PipelineHandler.Producer.class));
    Assertions.assertFalse(
        PipelineHandler.filter(Map.of(PipelineHandler.ACTIVE_KEY, "false")).test(tp));
    Assertions.assertTrue(
        PipelineHandler.filter(Map.of(PipelineHandler.ACTIVE_KEY, "true")).test(tp));
    Assertions.assertTrue(PipelineHandler.filter(Map.of()).test(tp));
  }

  @Test
  void testGetPipeline() throws InterruptedException, ExecutionException {
    var topic = Utils.randomString(10);
    try (var admin = Admin.of(bootstrapServers())) {
      admin.creator().topic(topic).numberOfPartitions(1).create();
      TimeUnit.SECONDS.sleep(2);
      try (var producer = Producer.of(bootstrapServers())) {
        producer.sender().topic(topic).value(new byte[10]).run().toCompletableFuture().get();
        var handler = new PipelineHandler(admin);
        var response =
            Assertions.assertInstanceOf(
                PipelineHandler.TopicPartitions.class, handler.get(Optional.empty(), Map.of()));
        Assertions.assertNotEquals(0, response.topicPartitions.size());
        Assertions.assertEquals(
            1, response.topicPartitions.stream().filter(t -> t.topic.equals(topic)).count());
      }
    }
  }
}
