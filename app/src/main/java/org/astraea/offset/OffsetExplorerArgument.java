package org.astraea.offset;

import com.beust.jcommander.Parameter;
import java.util.Set;
import org.astraea.argument.ArgumentUtil;

public class OffsetExplorerArgument {
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
      converter = ArgumentUtil.SetConverter.class)
  public Set<String> topic;
}
