package org.astraea.offset;

import com.beust.jcommander.Parameter;
import java.util.Collections;
import java.util.Set;
import org.astraea.argument.ArgumentUtil;

public class OffsetExplorerArgument {
  @Parameter(
      names = {"--bootstrap.servers"},
      description = "String: server to connect to",
      validateWith = ArgumentUtil.NotEmptyString.class,
      required = true)
  String brokers;

  @Parameter(
      names = {"--topics"},
      description = "String: topic names, comma separate",
      validateWith = ArgumentUtil.NotEmptyString.class,
      converter = ArgumentUtil.StringSetConverter.class)
  Set<String> topics = Collections.emptySet();
}
