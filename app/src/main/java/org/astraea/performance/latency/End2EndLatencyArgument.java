package org.astraea.performance.latency;

import com.beust.jcommander.Parameter;
import java.time.Duration;
import org.astraea.argument.ArgumentUtil;

public class End2EndLatencyArgument {
  @Parameter(
      names = {"--bootstrap.servers"},
      description = "String: server to connect to",
      validateWith = ArgumentUtil.NotEmptyString.class,
      required = true)
  String brokers;

  @Parameter(
      names = {"--topic"},
      description = "String: topic name",
      validateWith = ArgumentUtil.NotEmptyString.class)
  String topic = "testLatency-" + System.currentTimeMillis();

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
      names = {"--valueSize"},
      description = "Integer: bytes per record sent",
      validateWith = ArgumentUtil.PositiveLong.class)
  int valueSize = 100;

  @Parameter(
      names = {"--flushDuration"},
      description = "Long: timeout for producer to flush the records",
      validateWith = ArgumentUtil.PositiveLong.class,
      converter = ArgumentUtil.DurationConverter.class)
  Duration flushDuration = Duration.ofSeconds(2);
}
