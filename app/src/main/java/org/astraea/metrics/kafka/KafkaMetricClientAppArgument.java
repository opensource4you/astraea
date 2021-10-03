package org.astraea.metrics.kafka;

import com.beust.jcommander.Parameter;
import java.util.List;
import org.astraea.argument.ArgumentUtil;

public class KafkaMetricClientAppArgument {
  @Parameter(
      names = {"--JMXServerAddress"},
      description = "The address to connect to JMX remote server",
      validateWith = ArgumentUtil.NotEmpty.class,
      required = true)
  public String address;

  @Parameter(
      names = {"--metrics"},
      variableArity = true,
      description =
          "The metric names you want. If no metric name specified in argument, all metrics will be selected.")
  public List<String> metrics;
}
