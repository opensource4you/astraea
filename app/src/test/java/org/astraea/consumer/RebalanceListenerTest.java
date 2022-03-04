package org.astraea.consumer;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.astraea.Utils;
import org.astraea.concurrent.State;
import org.astraea.concurrent.ThreadPool;
import org.astraea.service.RequireBrokerCluster;
import org.junit.jupiter.api.Test;

public class RebalanceListenerTest extends RequireBrokerCluster {
  @Test
  void testConsumerRebalanceListener() {
    var getAssignment = new AtomicInteger(0);
    var topicName = "testRebalanceListener-" + System.currentTimeMillis();
    try (var consumer =
        Consumer.builder()
            .brokers(bootstrapServers())
            .topics(Set.of(topicName))
            .consumerRebalanceListener(ignore -> getAssignment.incrementAndGet())
            .build()) {
      try (var threadPool =
          ThreadPool.builder()
              .executor(
                  () -> {
                    consumer.poll(Duration.ofSeconds(10));
                    return State.DONE;
                  })
              .build()) {
        Utils.waitFor(() -> getAssignment.get() == 1, Duration.ofSeconds(10));
      }
    }
  }
}
