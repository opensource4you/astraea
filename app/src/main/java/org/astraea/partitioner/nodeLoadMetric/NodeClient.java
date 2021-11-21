package org.astraea.partitioner.nodeLoadMetric;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.management.remote.JMXServiceURL;
import org.astraea.metrics.jmx.MBeanClient;
import org.astraea.metrics.kafka.BrokerTopicMetricsResult;
import org.astraea.metrics.kafka.KafkaMetrics;

/** Responsible for connecting jmx according to the received address */
public class NodeClient implements NodeMetadata, AutoCloseable {
  private final String JMX_URI_FORMAT = "service:jmx:rmi:///jndi/rmi://" + "%s" + "/jmxrmi";
  private final JMXServiceURL serviceURL;
  private final MBeanClient mBeanClient;
  private final String nodeID;
  private Collection<String> argumentTargetMetrics = new ArrayList<>();

  private double totalBytes;
  private int overLoadCount;

  NodeClient(String ID, String address) throws IOException {
    argumentTargetMetrics.add("BytesInPerSec");
    argumentTargetMetrics.add("BytesOutPerSec");
    nodeID = ID;
    if (Pattern.compile("^service:").matcher(address).find())
      serviceURL = new JMXServiceURL(address);
    else serviceURL = new JMXServiceURL(createJmxUrl(address));
    mBeanClient = new MBeanClient(serviceURL);
    totalBytes = 0.0;
    overLoadCount = 0;
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

    setTotalBytes(
        collect.stream()
            .mapToDouble(s -> (double) s.beanObject().getAttributes().get("MeanRate"))
            .reduce(Double::sum)
            .stream()
            .sum());
  }

  public MBeanClient getKafkaMetricClient() {
    return mBeanClient;
  }

  public void setOverLoadCount(int count) {
    this.overLoadCount = count;
  }

  public void setTotalBytes(double bytes) {
    this.totalBytes = bytes;
  }

  public double getTotalBytes() {
    return this.totalBytes;
  }

  public String getNodeID() {
    return this.nodeID;
  }

  public int getOverLoadCount() {
    return this.overLoadCount;
  }

  @Override
  public void close() {
    MBeanClient mBeanClient = getKafkaMetricClient();
    try {
      mBeanClient.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
