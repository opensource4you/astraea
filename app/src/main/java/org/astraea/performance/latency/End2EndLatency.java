package org.astraea.performance.latency;

import com.beust.jcommander.Parameter;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.astraea.argument.ArgumentUtil;
import org.astraea.argument.BasicArgument;
import org.astraea.concurrent.ThreadPool;

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

  static ThreadPool.Executor producerThread(
      DataManager dataManager, MeterTracker tracker, Producer producer, Duration flushDuration) {
    return new ThreadPool.Executor() {
      private long lastSend = 0;

      @Override
      public void execute() throws InterruptedException {
        var records = dataManager.producerRecords();
        var now = System.currentTimeMillis();
        dataManager.sendingRecord(records, now);
        records.forEach(
            record ->
                producer
                    .send(record)
                    .whenComplete(
                        (r, e) -> {
                          if (e != null) tracker.record(0, System.currentTimeMillis() - now);
                          else
                            tracker.record(
                                r.serializedKeySize() + r.serializedValueSize(),
                                System.currentTimeMillis() - now);
                        }));
        if (lastSend <= 0) lastSend = now;
        else if (lastSend + flushDuration.toMillis() < now) {
          lastSend = now;
          producer.flush();
        }
      }

      @Override
      public void cleanup() {
        producer.close();
      }
    };
  }

  static ThreadPool.Executor consumerExecutor(
      DataManager dataManager, MeterTracker tracker, Consumer consumer) {
    return new ThreadPool.Executor() {
      @Override
      public void execute() throws InterruptedException {
        try {
          var now = System.currentTimeMillis();
          var records = consumer.poll();
          records.forEach(
              record -> {
                var entry = dataManager.removeSendingRecord(record.key());
                var latency = now - entry.getValue();
                var produceRecord = entry.getKey();
                if (!KafkaUtils.equal(produceRecord, record))
                  System.out.println("receive corrupt data!!!");
                else
                  tracker.record(
                      record.serializedKeySize() + record.serializedValueSize(), latency);
              });
        } catch (org.apache.kafka.common.errors.WakeupException e) {
          throw new InterruptedException(e.getMessage());
        }
      }

      @Override
      public void cleanup() {
        consumer.close();
      }

      @Override
      public void wakeup() {
        consumer.wakeup();
      }
    };
  }

  static AutoCloseable execute(ComponentFactory factory, Argument parameters) throws Exception {
    var consumerTracker = new MeterTracker("consumer latency");
    var producerTracker = new MeterTracker("producer latency");
    var dataManager =
        parameters.numberOfConsumers <= 0
            ? DataManager.noConsumer(parameters.topics, parameters.valueSize)
            : DataManager.of(parameters.topics, parameters.valueSize);

    try (var topicAdmin = factory.topicAdmin()) {
      // the number of partitions is equal to number of consumers. That make each consumer can
      // consume a part of topic.
      parameters.topics.forEach(
          topic ->
              topicAdmin.createTopic(
                  topic, parameters.numberOfConsumers <= 0 ? 1 : parameters.numberOfConsumers));
    }

    return ThreadPool.builder()
        .executor(consumerTracker)
        .executor(producerTracker)
        .executors(
            IntStream.range(0, parameters.numberOfProducers)
                .mapToObj(
                    i ->
                        producerThread(
                            dataManager,
                            producerTracker,
                            factory.producer(),
                            parameters.flushDuration))
                .collect(Collectors.toList()))
        .executors(
            IntStream.range(0, parameters.numberOfConsumers)
                .mapToObj(i -> consumerExecutor(dataManager, consumerTracker, factory.consumer()))
                .collect(Collectors.toList()))
        .build();
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
