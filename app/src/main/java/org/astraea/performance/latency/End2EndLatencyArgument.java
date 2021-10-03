package org.astraea.performance.latency;

import com.beust.jcommander.Parameter;
import java.time.Duration;
import org.astraea.argument.ArgumentUtil;

public class End2EndLatencyArgument {

  static final String BROKERS_KEY = "--bootstrap.servers";
  static final String TOPIC_KEY = "--topic";
  static final String PRODUCERS_KEY = "--producers";
  static final String CONSUMERS_KEY = "--consumers";
  static final String DURATION_KEY = "--duration";
  static final String VALUE_SIZE_KEY = "--valueSize";
  static final String FLUSH_DURATION_KEY = "--flushDuration";

  @Parameter(
      names = {BROKERS_KEY},
      description = "String: server to connect to",
      validateWith = ArgumentUtil.NotEmpty.class,
      required = true)
  String brokers;

  @Parameter(
      names = {TOPIC_KEY},
      description = "String: topic name",
      validateWith = ArgumentUtil.NotEmpty.class)
  String topic = "testLatency-" + System.currentTimeMillis();

  @Parameter(
      names = {PRODUCERS_KEY},
      description = "Integer: number of producers to create",
      validateWith = ArgumentUtil.LongPositive.class)
  int numberOfProducers = 1;

  @Parameter(
      names = {CONSUMERS_KEY},
      description = "Integer: number of consumers to create",
      validateWith = ArgumentUtil.LongNotNegative.class)
  int numberOfConsumers = 1;

  @Parameter(
      names = {DURATION_KEY},
      description = "Long: producing time in seconds",
      validateWith = ArgumentUtil.LongPositive.class,
      converter = ArgumentUtil.DurationConverter.class)
  Duration duration = Duration.ofSeconds(5);

  @Parameter(
      names = {VALUE_SIZE_KEY},
      description = "Integer: bytes per record sent",
      validateWith = ArgumentUtil.LongPositive.class)
  int valueSize = 100;

  @Parameter(
      names = {FLUSH_DURATION_KEY},
      description = "Long: timeout for producer to flush the records",
      validateWith = ArgumentUtil.LongPositive.class,
      converter = ArgumentUtil.DurationConverter.class)
  Duration flushDuration = Duration.ofSeconds(2);
}
