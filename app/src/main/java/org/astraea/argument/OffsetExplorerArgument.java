package org.astraea.argument;

import com.beust.jcommander.Parameter;

public class OffsetExplorerArgument {
  @Parameter(
      names = {"--bootstrap.servers"},
      description = "String: server to connect to",
      validateWith = ArgumentCheck.NotEmpty.class,
      required = true)
  public String brokers;

  @Parameter(
      names = {"--topic"},
      description = "String: topic name",
      validateWith = ArgumentCheck.NotEmpty.class,
      required = true)
  public String topic;
}
