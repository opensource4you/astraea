package org.astraea.performance.latency;

import com.beust.jcommander.Parameter;
import java.time.Duration;
import org.astraea.argument.ArgumentUtil;

public class End2EndLatencyArgument {
  @Parameter(
      names = {"--bootstrap.servers"},
      description = "String: server to connect to",
      validateWith = ArgumentUtil.NotEmpty.class,
      required = true)
  public String brokers;

  @Parameter(
      names = {"--topic"},
      description = "String: topic name",
      validateWith = ArgumentUtil.NotEmpty.class,
      required = true)
  public String topic;

  @Parameter(
      names = {"--producers"},
      description = "Integer: number of producers to create",
      validateWith = ArgumentUtil.LongPositive.class)
  public int producers = 1;

  @Parameter(
      names = {"--consumers"},
      description = "Integer: number of consumers to create",
      validateWith = ArgumentUtil.LongNotNegative.class)
  public int consumers = 1;

  @Parameter(
      names = {"--duration"},
      description = "Long: producing time in seconds",
      validateWith = ArgumentUtil.LongPositive.class,
      converter = ArgumentUtil.DurationConverter.class)
  public Duration duration = Duration.ofSeconds(5);

  @Parameter(
      names = {"--valueSize"},
      description = "Integer: bytes per record sent",
      validateWith = ArgumentUtil.LongPositive.class)
  public int valueSize = 100;

  @Parameter(
      names = {"--flushDuration"},
      description = "Long: timeout for producer to flush the records",
      validateWith = ArgumentUtil.LongPositive.class,
      converter = ArgumentUtil.DurationConverter.class)
  public Duration flushDuration = Duration.ofSeconds(2);
}
