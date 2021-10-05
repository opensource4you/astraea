package org.astraea.metrics.kafka;

import com.beust.jcommander.Parameter;
import java.util.Set;
import org.astraea.argument.ArgumentUtil;
import org.astraea.metrics.kafka.metrics.BrokerTopicMetrics;

public class KafkaMetricClientAppArgument {

  @Parameter(
      names = {"--jmx.server"},
      description = "The address to connect to JMX remote server",
      validateWith = ArgumentUtil.NotEmptyString.class,
      required = true)
  String address;

  @Parameter(
      names = {"--metrics"},
      variableArity = true,
      converter = ArgumentUtil.SetConverter.class,
      description =
          "The metric names you want. If no metric name specified in argument, all metrics will be selected.")
  Set<String> metrics = BrokerTopicMetrics.allMetricNames;
}
