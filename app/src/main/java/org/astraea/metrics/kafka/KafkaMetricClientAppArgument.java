package org.astraea.metrics.kafka;

import com.beust.jcommander.Parameter;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import org.astraea.argument.ArgumentUtil;
import org.astraea.metrics.kafka.metrics.BrokerTopicMetrics;

public class KafkaMetricClientAppArgument {
  static final Set<String> allMetrics;

  static {
    allMetrics =
        Arrays.stream(BrokerTopicMetrics.values())
            .map(BrokerTopicMetrics::metricName)
            .collect(Collectors.toUnmodifiableSet());
  }

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
  Set<String> metrics = allMetrics;
}
