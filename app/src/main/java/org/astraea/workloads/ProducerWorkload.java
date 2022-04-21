package org.astraea.workloads;

import java.util.Map;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;

public interface ProducerWorkload<KeyType, ValueType> extends Runnable {

  default Producer<KeyType, ValueType> offerClient(Map<String, Object> configs) {
    return new KafkaProducer<>(configs);
  }

  @Override
  default void run() {
    try (var producer = offerClient(offerConfigs())) {
      run(producer);
    } catch (Exception ex) {
      ex.printStackTrace();
    }
  }

  /**
   * Offers a configuration map for producer initialization
   *
   * @return the configuration for producer initialization
   */
  Map<String, Object> offerConfigs();

  /**
   * Encapsulate the logic of specific application workload. This method will use the given producer
   * to simulate the specific client workload. Once this method exited, the workload is considered
   * finished. This method might be running inside a standalone thread. To offer a way to stop this
   * workload, the implementation should constantly check the {@link Thread#isInterrupted()} state.
   * Once the thread is interrupted, the simulation execution should stop as soon as possible.
   *
   * @param producer the producer for workload simulation
   */
  void run(Producer<KeyType, ValueType> producer);
}
