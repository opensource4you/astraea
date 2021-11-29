package org.astraea.partitioner.nodeLoadMetric;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.management.remote.JMXServiceURL;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.astraea.metrics.jmx.BeanCollector;
import org.astraea.metrics.jmx.MBeanClient;
import org.astraea.metrics.kafka.KafkaMetrics;

public class NodeLoadClient implements AutoCloseable {

  private static final String JMX_URI_FORMAT = "service:jmx:rmi:///jndi/rmi://" + "%s" + "/jmxrmi";
  private final OverLoadNode overLoadNode;
  private final LoadPoisson loadPoisson;
  private final BeanCollector beanCollector = new BeanCollector();
  private final Collection<String> currentJmxAddresses;

  public NodeLoadClient(Collection<String> jmxAddresses) {
    currentJmxAddresses = jmxAddresses;
    this.overLoadNode = new OverLoadNode(beanCollector);
    loadPoisson = new LoadPoisson();
  }

  public void addNodeMetrics() throws IOException {
    for (String address : currentJmxAddresses) {
      JMXServiceURL serviceURL;
      if (Pattern.compile("^service:").matcher(address).find())
        serviceURL = new JMXServiceURL(address);
      else serviceURL = new JMXServiceURL(String.format(JMX_URI_FORMAT, address));
      MBeanClient mBean = new MBeanClient(serviceURL);
      beanCollector.addClient(mBean, KafkaMetrics.BrokerTopic.BytesOutPerSec::fetch);
      beanCollector.addClient(mBean, KafkaMetrics.BrokerTopic.BytesInPerSec::fetch);
    }
  }

  public void nodesPoisson(Cluster cluster) {
    var addresses =
        cluster.nodes().stream()
            .collect(Collectors.toMap(Node::host, Node::port, (r1, r2) -> r1))
            .entrySet();
    var metrics = overLoadNode.nodesOverLoad(addresses);
    var idMetrics = new HashMap<Integer, Integer>();
    for (Node node : cluster.nodes()) {
      metrics.entrySet().stream()
          .filter(
              entry ->
                  node.host().equals(entry.getKey().getKey())
                      && node.port() == entry.getKey().getValue())
          .findAny()
          .map(n -> idMetrics.put(node.id(), n.getValue()));
    }
    loadPoisson.allNodesPoisson(idMetrics);
  }

  @Override
  public void close() throws Exception {
    beanCollector.close();
  }

  public LoadPoisson getLoadPoisson() {
    return loadPoisson;
  }
}
