package org.astraea.workloads;

import java.time.Duration;
import java.util.Collections;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.astraea.workloads.annotations.NamedArg;

public final class ConsumerWorkloads {

  private ConsumerWorkloads() {}

  public static ConsumerWorkload<?, ?> straightWorkload(
      @NamedArg(name = "bootstrapServer") String bootstrapServer,
      @NamedArg(name = "consumerGroupId") String consumerGroupId,
      @NamedArg(name = "topicName") String topicName) {
    return ConsumerWorkloadBuilder.builder()
        .bootstrapServer(bootstrapServer)
        .consumerGroupId(consumerGroupId)
        .keyDeserializer(BytesDeserializer.class)
        .valueDeserializer(BytesDeserializer.class)
        .build(
            (consumer) -> {
              while (!Thread.currentThread().isInterrupted()) {
                consumer.subscribe(Collections.singleton(topicName));
                consumer.poll(Duration.ofMillis(100));
              }
            });
  }
}
