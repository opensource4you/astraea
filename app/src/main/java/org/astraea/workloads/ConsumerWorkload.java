package org.astraea.workloads;

import java.util.Map;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public interface ConsumerWorkload<KeyType, ValueType> extends Runnable {

  default Consumer<KeyType, ValueType> offerClient(Map<String, Object> configs) {
    return new KafkaConsumer<>(configs);
  }

  @Override
  default void run() {
    try (var consumer = offerClient(offerConfigs())) {
      run(consumer);
    } catch (Exception e) {
      e.printStackTrace();
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
  void run(Consumer<KeyType, ValueType> producer);
}
