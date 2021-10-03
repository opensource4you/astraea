package org.astraea.performance.latency;

import java.util.Collections;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.astraea.argument.ArgumentUtil;

public class End2EndLatency {

  static End2EndLatencyArgument parameters(String[] args) {
    var parameters = new End2EndLatencyArgument();
    ArgumentUtil.parseArgument(parameters, args);
    return parameters;
  }

  public static void main(String[] args) throws Exception {
    var parameters = parameters(args);

    System.out.println("brokers: " + parameters.brokers);
    System.out.println("topic: " + parameters.topic);
    System.out.println("numberOfProducers: " + parameters.numberOfProducers);
    System.out.println("numberOfConsumers: " + parameters.numberOfConsumers);
    System.out.println("duration: " + parameters.duration.toSeconds() + " seconds");
    System.out.println("value size: " + parameters.valueSize + " bytes");
    System.out.println("flush duration: " + parameters.flushDuration.toSeconds() + " seconds");

    try (var closeFlag =
        execute(
            ComponentFactory.fromKafka(parameters.brokers, Collections.singleton(parameters.topic)),
            parameters)) {
      TimeUnit.MILLISECONDS.sleep(parameters.duration.toMillis());
    }
  }

  static AutoCloseable execute(ComponentFactory factory, End2EndLatencyArgument parameters)
      throws Exception {
    var consumerTracker = new MeterTracker("consumer latency");
    var producerTracker = new MeterTracker("producer latency");
    var dataManager =
        parameters.numberOfConsumers <= 0
            ? DataManager.noConsumer(parameters.topic, parameters.valueSize)
            : DataManager.of(parameters.topic, parameters.valueSize);

    // create producer threads
    var producers =
        IntStream.range(0, parameters.numberOfProducers)
            .mapToObj(
                i ->
                    new ProducerThread(
                        dataManager, producerTracker, factory.producer(), parameters.flushDuration))
            .collect(Collectors.toList());

    // create consumers threads
    var consumers =
        IntStream.range(0, parameters.numberOfConsumers)
            .mapToObj(
                i -> new ConsumerThread(dataManager, consumerTracker, factory.createConsumer()))
            .collect(Collectors.toList());

    // + 2 for latency trackers
    var services =
        Executors.newFixedThreadPool(
            parameters.numberOfProducers + parameters.numberOfConsumers + 2);

    AutoCloseable releaseAllObjects =
        () -> {
          producerTracker.close();
          consumerTracker.close();
          producers.forEach(ProducerThread::close);
          consumers.forEach(ConsumerThread::close);
          services.shutdownNow();
          if (!services.awaitTermination(30, TimeUnit.SECONDS)) {
            System.out.println("timeout to wait all threads");
          }
        };
    try {
      try (var topicAdmin = factory.createTopicAdmin()) {
        // the number of partitions is equal to number of consumers. That make each consumer can
        // consume a part of topic.
        KafkaUtils.createTopicIfNotExist(
            topicAdmin,
            parameters.topic,
            parameters.numberOfConsumers <= 0 ? 1 : parameters.numberOfConsumers);
      }
      consumers.forEach(services::execute);
      producers.forEach(services::execute);
      services.execute(consumerTracker);
      services.execute(producerTracker);
      return releaseAllObjects;
    } catch (Exception e) {
      releaseAllObjects.close();
      throw e;
    }
  }
}
