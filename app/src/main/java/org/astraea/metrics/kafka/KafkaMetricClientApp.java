package org.astraea.metrics.kafka;

import java.net.MalformedURLException;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.FormatStyle;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.management.remote.JMXServiceURL;
import org.astraea.argument.ArgumentUtil;
import org.astraea.metrics.kafka.metrics.BrokerTopicMetrics;
import org.astraea.metrics.kafka.metrics.BrokerTopicMetricsResult;

public final class KafkaMetricClientApp {

  public static String JMX_URI_FORMAT = "service:jmx:rmi:///jndi/rmi://" + "%s" + "/jmxrmi";

  public static String createJmxUrl(String address) {
    return String.format(JMX_URI_FORMAT, address);
  }

  public static void main(String[] args) throws MalformedURLException {
    var parameters = new KafkaMetricClientAppArgument();
    ArgumentUtil.parseArgument(parameters, args);

    String argumentJmxServerNetworkAddress = parameters.address;
    List<String> argumentTargetMetrics = parameters.metrics;

    JMXServiceURL serviceURL = new JMXServiceURL(createJmxUrl(argumentJmxServerNetworkAddress));
    try (KafkaMetricClient kafkaMetricClient = new KafkaMetricClient(serviceURL)) {

      // find the actual metrics to fetch.
      List<BrokerTopicMetrics> metrics =
          argumentTargetMetrics.stream()
              .map(BrokerTopicMetrics::valueOf)
              .collect(Collectors.toUnmodifiableList());

      // if no metric name specified, all metrics are selected
      if (argumentTargetMetrics.size() == 0) metrics = List.of(BrokerTopicMetrics.values());

      while (!Thread.interrupted()) {
        // fetch
        List<BrokerTopicMetricsResult> collect =
            metrics.stream()
                .map(kafkaMetricClient::requestMetric)
                .collect(Collectors.toUnmodifiableList());

        // output
        System.out.println(
            "["
                + LocalTime.now().format(DateTimeFormatter.ofLocalizedTime(FormatStyle.MEDIUM))
                + "]");
        for (BrokerTopicMetricsResult result : collect) System.out.println(result);
        System.out.println();

        try {
          TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e) {
          break;
        }
      }

    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
