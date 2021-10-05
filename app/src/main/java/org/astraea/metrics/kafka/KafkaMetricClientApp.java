package org.astraea.metrics.kafka;

import java.net.MalformedURLException;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.FormatStyle;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.management.remote.JMXServiceURL;
import org.astraea.metrics.kafka.metrics.BrokerTopicMetrics;
import org.astraea.metrics.kafka.metrics.BrokerTopicMetricsResult;

public final class KafkaMetricClientApp {

  public static String JMX_URI_FORMAT = "service:jmx:rmi:///jndi/rmi://" + "%s" + "/jmxrmi";

  public static String createJmxUrl(String address) {
    return String.format(JMX_URI_FORMAT, address);
  }

  public static void main(String[] args) throws MalformedURLException {

//     ensure argument safe
    if (args.length < 1) {
      help();
      throw new IllegalArgumentException();
    }
    String argumentJmxServerNetworkAddress = args[0];
    List<String> argumentTargetMetrics = List.of(args).subList(1, args.length);
    String[] test = {"localhost:18685", "BytesInPerSec"};


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
      help();
    }
  }

  private static void help() {
    String simpleName = KafkaMetricClientApp.class.getSimpleName();
    System.err.printf("Usage: %s <jmx server address> [metric names ...]\n", simpleName);
    System.err.println();
    System.err.println("If no metric name specified in argument, all metrics will be selected.");
    System.err.println();
    System.err.printf("Example 1: %s localhost:9875\n", simpleName);
    System.err.printf("Example 2: %s localhost:9875 BytesInPerSec BytesOutPerSec\n", simpleName);
    System.err.println();
    System.err.println("Available Metrics:");
    for (BrokerTopicMetrics value : BrokerTopicMetrics.values()) {
      System.err.printf("    %s\n", value.metricName());
    }
  }
}
