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
  String brokers;

  @Parameter(
      names = {"--topics"},
      description = "String: topic names, comma separate",
      validateWith = ArgumentUtil.NotEmpty.class,
      converter = ArgumentUtil.SetConverter.class)
  Set<String> topics;
}
