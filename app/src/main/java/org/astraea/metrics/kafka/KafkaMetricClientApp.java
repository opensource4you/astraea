package org.astraea.metrics.kafka;

import com.beust.jcommander.Parameter;
import java.net.MalformedURLException;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.FormatStyle;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.management.remote.JMXServiceURL;
import org.astraea.argument.ArgumentUtil;
import org.astraea.metrics.jmx.MBeanClient;
import org.astraea.metrics.kafka.metrics.BrokerTopicMetricsResult;

public final class KafkaMetricClientApp {

  public static String JMX_URI_FORMAT = "service:jmx:rmi:///jndi/rmi://" + "%s" + "/jmxrmi";

  public static String createJmxUrl(String address) {
    return String.format(JMX_URI_FORMAT, address);
  }

  public static void main(String[] args) throws MalformedURLException {
    var parameters = ArgumentUtil.parseArgument(new Argument(), args);

    String argumentJmxServerNetworkAddress = parameters.address;
    Set<String> argumentTargetMetrics = parameters.metrics;

    JMXServiceURL serviceURL = new JMXServiceURL(createJmxUrl(argumentJmxServerNetworkAddress));
    try (MBeanClient mBeanClient = new MBeanClient(serviceURL)) {

      // find the actual metrics to fetch.
      List<KafkaMetrics.BrokerTopic> metrics =
          argumentTargetMetrics.stream()
              .map(KafkaMetrics.BrokerTopic::of)
              .collect(Collectors.toUnmodifiableList());

      while (!Thread.interrupted()) {
        // fetch
        List<BrokerTopicMetricsResult> collect =
            metrics.stream()
                .map(x -> x.fetch(mBeanClient))
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

  static class Argument {
    @Parameter(
        names = {"--jmx.server"},
        description = "The address to connect to JMX remote server",
        validateWith = ArgumentUtil.NotEmptyString.class,
        required = true)
    String address;

    @Parameter(
        names = {"--metrics"},
        variableArity = true,
        converter = ArgumentUtil.StringSetConverter.class,
        description =
            "The metric names you want. If no metric name specified in argument, all metrics will be selected.")
    Set<String> metrics =
        Arrays.stream(KafkaMetrics.BrokerTopic.values())
            .map(KafkaMetrics.BrokerTopic::metricName)
            .collect(Collectors.toUnmodifiableSet());
  }
}
