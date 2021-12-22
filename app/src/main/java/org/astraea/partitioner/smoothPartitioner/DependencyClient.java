package org.astraea.partitioner.smoothPartitioner;

import org.apache.kafka.clients.producer.KafkaProducer;

/**
 * A DependencyClient allows users to make producers send dependency data.
 *
 * <pre>{@code
 * KafkaProducer producer = new KafkaProducer(props);
 * var dependencyClient = new DependencyClient(producer);
 * dependencyClient.initializeDependency();
 * dependencyClient.beginDependency();
 * producer.send();
 * dependencyClient.finishDependency();
 * }</pre>
 */
public class DependencyClient {
  private final SmoothWeightPartitioner smoothWeightPartitioner;

  public DependencyClient(KafkaProducer kafkaProducer)
      throws NoSuchFieldException, IllegalAccessException {
    this.smoothWeightPartitioner = partitionerOfProducer(kafkaProducer);
  }

  public synchronized void initializeDependency() {
    smoothWeightPartitioner.initializeDependency();
  }

  public synchronized void beginDependency() {
    smoothWeightPartitioner.beginDependency();
  }

  public synchronized void finishDependency() {
    smoothWeightPartitioner.finishDependency();
  }

  private SmoothWeightPartitioner partitionerOfProducer(KafkaProducer producer)
      throws NoSuchFieldException, IllegalAccessException {
    var field = producer.getClass().getDeclaredField("partitioner");
    field.setAccessible(true);
    return (SmoothWeightPartitioner) field.get(producer);
  }
}
