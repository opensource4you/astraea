package org.astraea.performance.latency;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.astraea.service.RequireBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class End2EndLatencyTest extends RequireBrokerCluster {

  @Test
  void testExecute() throws Exception {
    var parameters = new End2EndLatency.Argument();
    parameters.brokers = "brokers";
    parameters.topics = Set.of("topic");
    parameters.numberOfProducers = 1;
    parameters.numberOfConsumers = 1;
    parameters.duration = Duration.ofSeconds(1);
    parameters.valueSize = 10;
    parameters.flushDuration = Duration.ofSeconds(1);
    var factory = ComponentFactory.of(bootstrapServers(), parameters.topics);
    try (var r = End2EndLatency.execute(factory, parameters)) {
      TimeUnit.SECONDS.sleep(2);
    }
    try (var consumer = factory.consumer()) {
      var records =
          IntStream.range(0, 3)
              .mapToObj(i -> consumer.poll())
              .flatMap(e -> e.partitions().stream())
              .collect(Collectors.toList());
      Assertions.assertEquals(0, records.size());
    }
  }
}
