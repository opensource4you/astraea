package org.astraea.partitioner.nodeLoadMetric;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.management.remote.JMXServiceURL;
import org.astraea.Utils;
import org.astraea.metrics.jmx.MBeanClient;
import org.astraea.metrics.kafka.BrokerTopicMetricsResult;
import org.astraea.metrics.kafka.KafkaMetrics;

/** Responsible for connecting jmx according to the received address */
public class NodeClient implements NodeMetadata, AutoCloseable {
  private static final String JMX_URI_FORMAT = "service:jmx:rmi:///jndi/rmi://" + "%s" + "/jmxrmi";
  private final JMXServiceURL serviceURL;
  private final MBeanClient mBeanClient;
  private final String nodeID;
  private static final Collection<String> BYTES_METRICS =
      Set.of(
          KafkaMetrics.BrokerTopic.BytesOutPerSec.toString(),
          KafkaMetrics.BrokerTopic.BytesOutPerSec.toString());

  private double totalBytes;
  private int overLoadCount;

  NodeClient(String ID, String address) throws IOException {
    nodeID = ID;
    if (Pattern.compile("^service:").matcher(address).find())
      serviceURL = new JMXServiceURL(address);
    else serviceURL = new JMXServiceURL(String.format(JMX_URI_FORMAT, address));
    mBeanClient = new MBeanClient(serviceURL);
    totalBytes = 0.0;
    overLoadCount = 0;
  }

  public void refreshMetrics() {
    List<KafkaMetrics.BrokerTopic> metrics =
        BYTES_METRICS.stream()
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

  public void setOverLoadCount(int count) {
    this.overLoadCount = count;
  }

  public void setTotalBytes(double bytes) {
    this.totalBytes = bytes;
  }

  public double totalBytes() {
    return this.totalBytes;
  }

  public String nodeID() {
    return this.nodeID;
  }

  public int overLoadCount() {
    return this.overLoadCount;
  }

  @Override
  public void close() {
    Utils.close(mBeanClient);
  }
}
