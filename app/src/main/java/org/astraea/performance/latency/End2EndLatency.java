package org.astraea.performance.latency;

import com.beust.jcommander.Parameter;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.astraea.argument.ArgumentUtil;
import org.astraea.argument.BasicArgument;

public class End2EndLatency {

  public static void main(String[] args) throws Exception {
    var parameters = ArgumentUtil.parseArgument(new Argument(), args);
    System.out.println("brokers: " + parameters.brokers);
    System.out.println("topics: " + parameters.topics);
    System.out.println("numberOfProducers: " + parameters.numberOfProducers);
    System.out.println("numberOfConsumers: " + parameters.numberOfConsumers);
    System.out.println("duration: " + parameters.duration.toSeconds() + " seconds");
    System.out.println("value size: " + parameters.valueSize + " bytes");
    System.out.println("flush duration: " + parameters.flushDuration.toSeconds() + " seconds");

    try (var closeFlag =
        execute(
            ComponentFactory.fromKafka(parameters.properties(), parameters.topics), parameters)) {
      TimeUnit.MILLISECONDS.sleep(parameters.duration.toMillis());
    }
  }

  static AutoCloseable execute(ComponentFactory factory, Argument parameters) throws Exception {
    var consumerTracker = new MeterTracker("consumer latency");
    var producerTracker = new MeterTracker("producer latency");
    var dataManager =
        parameters.numberOfConsumers <= 0
            ? DataManager.noConsumer(parameters.topics, parameters.valueSize)
            : DataManager.of(parameters.topics, parameters.valueSize);

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
            parameters.topics,
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

  static class Argument extends BasicArgument {
    @Parameter(
        names = {"--topics"},
        description = "The topics to push/subscribe data",
        validateWith = ArgumentUtil.NotEmptyString.class,
        converter = ArgumentUtil.StringSetConverter.class)
    public Set<String> topics = Set.of("test-latency-" + System.currentTimeMillis());

    @Parameter(
        names = {"--producers"},
        description = "Integer: number of producers to create",
        validateWith = ArgumentUtil.PositiveLong.class)
    int numberOfProducers = 1;

    @Parameter(
        names = {"--consumers"},
        description = "Integer: number of consumers to create",
        validateWith = ArgumentUtil.NonNegativeLong.class)
    int numberOfConsumers = 1;

    @Parameter(
        names = {"--duration"},
        description = "Long: producing time in seconds",
        validateWith = ArgumentUtil.PositiveLong.class,
        converter = ArgumentUtil.DurationConverter.class)
    Duration duration = Duration.ofSeconds(5);

    @Parameter(
        names = {"--value.size"},
        description = "Integer: bytes per record sent",
        validateWith = ArgumentUtil.PositiveLong.class)
    int valueSize = 100;

    @Parameter(
        names = {"--flush.duration"},
        description = "Long: timeout for producer to flush the records",
        validateWith = ArgumentUtil.PositiveLong.class,
        converter = ArgumentUtil.DurationConverter.class)
    Duration flushDuration = Duration.ofSeconds(2);

    Map<String, Object> properties() {
      return properties(null);
    }
  }
}
