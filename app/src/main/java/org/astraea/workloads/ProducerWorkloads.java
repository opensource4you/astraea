package org.astraea.workloads;

import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.BytesSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.astraea.workloads.annotations.NamedArg;

public final class ProducerWorkloads {

  private ProducerWorkloads() {}

  public static ProducerWorkload<?, ?> powImbalanceWorkload(
      @NamedArg(name = "bootstrapServer") String bootstrapServer,
      @NamedArg(name = "topicName") String topicName,
      @NamedArg(name = "power") double power,
      @NamedArg(
              name = "ceiling",
              description = "reset the proportion counter once it reach the ceiling")
          double ceiling,
      @NamedArg(name = "recordSize") int recordSize,
      @NamedArg(name = "iterationWaitMs") int iterationWaitMs) {
    return ProducerWorkloadBuilder.builder()
        .bootstrapServer(bootstrapServer)
        .keySerializer(BytesSerializer.class)
        .valueSerializer(BytesSerializer.class)
        .build(
            (producer) -> {
              final Bytes bytes = Bytes.wrap(new byte[recordSize]);
              while (!Thread.currentThread().isInterrupted()) {
                final int partitionSize = producer.partitionsFor(topicName).size();

                // the proportion of each partition is (1):(rate):(rate^2):(rate^3)...
                // if the value exceeded the overflow point, the proportion is reset to 1
                double proportion = 1;
                for (int i = 0; i < partitionSize; i++, proportion *= power) {
                  if (proportion > ceiling) proportion = 1;
                  for (int j = 0; j < proportion; j++) {
                    producer.send(new ProducerRecord<>(topicName, i, null, bytes));
                  }
                }
                try {
                  TimeUnit.MILLISECONDS.sleep(iterationWaitMs);
                } catch (InterruptedException e) {
                  e.printStackTrace();
                  break;
                }
              }
            });
  }
}
