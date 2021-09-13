package org.astraea.performance.latency;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class End2EndLatency {

  static final String BROKERS_KEY = "--bootstrap.servers";
  static final String TOPIC_KEY = "--topic";
  static final String PRODUCERS_KEY = "--producers";
  static final String CONSUMERS_KEY = "--consumers";
  static final String DURATION_KEY = "--duration";
  static final String VALUE_SIZE_KEY = "--valueSize";
  static final String FLUSH_DURATION_KEY = "--flushDuration";

  private static IllegalArgumentException exception(String key, Object incorrectValue) {
    return new IllegalArgumentException(
        "the value: " + incorrectValue + " is illegal. Please use another value for " + key);
  }

  static class Parameters {
    final String brokers;
    final String topic;
    final int numberOfProducers;
    final int numberOfConsumers;
    final Duration duration;
    final int valueSize;
    final Duration flushDuration;

    Parameters(
        String brokers,
        String topic,
        int numberOfProducers,
        int numberOfConsumers,
        Duration duration,
        int valueSize,
        Duration flushDuration) {
      this.brokers = Objects.requireNonNull(brokers);
      this.topic = Objects.requireNonNull(topic);
      this.numberOfProducers = numberOfProducers;
      this.numberOfConsumers = numberOfConsumers;
      this.duration = duration;
      this.valueSize = valueSize;
      this.flushDuration = flushDuration;

      if (brokers.isBlank()) throw exception(BROKERS_KEY, brokers);
      if (topic.isBlank()) throw exception(TOPIC_KEY, topic);
      if (numberOfProducers <= 0) throw exception(PRODUCERS_KEY, numberOfProducers);
      if (numberOfConsumers < 0) throw exception(CONSUMERS_KEY, numberOfConsumers);
      if (duration.toMillis() <= 0) throw exception(DURATION_KEY, duration);
      if (flushDuration.toMillis() <= 0) throw exception(FLUSH_DURATION_KEY, flushDuration);
      if (valueSize <= 0) throw exception(VALUE_SIZE_KEY, valueSize);
    }
  }

  static Parameters parameters(String[] args) {
    // group arguments by key
    var argMap = new HashMap<String, String>();
    for (var i = 0; i <= args.length; i += 2) {
      if (i + 1 >= args.length) break;
      argMap.put(args[i], args[i + 1]);
    }

    // take values from legal keys. We just ignore the invalid configs.
    return new Parameters(
        argMap.getOrDefault(BROKERS_KEY, ""),
        argMap.getOrDefault(TOPIC_KEY, "testLatency-" + System.currentTimeMillis()),
        Integer.parseInt(argMap.getOrDefault(PRODUCERS_KEY, "1")),
        Integer.parseInt(argMap.getOrDefault(CONSUMERS_KEY, "1")),
        Duration.ofSeconds(Long.parseLong(argMap.getOrDefault(DURATION_KEY, "5"))),
        Integer.parseInt(argMap.getOrDefault(VALUE_SIZE_KEY, "100")),
        Duration.ofSeconds(Long.parseLong(argMap.getOrDefault(FLUSH_DURATION_KEY, "2"))));
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

  static AutoCloseable execute(ComponentFactory factory, Parameters parameters) throws Exception {
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
