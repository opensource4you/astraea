package org.astraea.partitioner.nodeLoadMetric;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.management.remote.JMXServiceURL;
import org.astraea.metrics.jmx.MBeanClient;
import org.astraea.metrics.kafka.BrokerTopicMetricsResult;
import org.astraea.metrics.kafka.KafkaMetrics;

/** Responsible for connecting jmx according to the received address */
public class NodeMetrics {
  private final String JMX_URI_FORMAT = "service:jmx:rmi:///jndi/rmi://" + "%s" + "/jmxrmi";
  private final JMXServiceURL serviceURL;
  private final MBeanClient mBeanClient;
  private final String nodeID;
  private HashMap<String, Double> metricsValues;
  private Collection<String> argumentTargetMetrics = new ArrayList<>();

  NodeMetrics(String ID, String address) throws IOException {
    argumentTargetMetrics.add("BytesInPerSec");
    argumentTargetMetrics.add("BytesOutPerSec");
    nodeID = ID;
    if (Pattern.compile("^service:").matcher(address).find())
      serviceURL = new JMXServiceURL(address);
    else serviceURL = new JMXServiceURL(createJmxUrl(address));
    mBeanClient = new MBeanClient(serviceURL);
    metricsValues = new HashMap();
  }

  public String createJmxUrl(String address) {
    return String.format(JMX_URI_FORMAT, address);
  }

  public void refreshMetrics() {
    List<KafkaMetrics.BrokerTopic> metrics =
        argumentTargetMetrics.stream()
            .map(KafkaMetrics.BrokerTopic::of)
            .collect(Collectors.toUnmodifiableList());

    List<BrokerTopicMetricsResult> collect =
        metrics.stream().map(x -> x.fetch(mBeanClient)).collect(Collectors.toUnmodifiableList());

    for (BrokerTopicMetricsResult result : collect) {
      metricsValues.put(
          result.beanObject().domainName(),
          (Double) result.beanObject().getAttributes().get("MeanRate"));
    }
  }

  public double totalBytesPerSec() {
    return metricsValues.get("BytesInPerSec") + metricsValues.get("BytesOutPerSec");
  }

  public MBeanClient getKafkaMetricClient() {
    return this.mBeanClient;
  }
}
