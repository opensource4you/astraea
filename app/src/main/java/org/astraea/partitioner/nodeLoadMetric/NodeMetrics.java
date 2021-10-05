package org.astraea.partitioner.nodeLoadMetric;

import java.net.MalformedURLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;
import javax.management.remote.JMXServiceURL;
import org.astraea.metrics.jmx.BeanObject;
import org.astraea.metrics.kafka.KafkaMetricClient;
import org.astraea.metrics.kafka.metrics.BrokerTopicMetrics;
import org.astraea.metrics.kafka.metrics.BrokerTopicMetricsResult;

/** Responsible for connecting jmx according to the received address */
public class NodeMetrics {
  public String JMX_URI_FORMAT = "service:jmx:rmi:///jndi/rmi://" + "%s" + "/jmxrmi";
  String[] metricsName = {"BytesInPerSec", "BytesOutPerSec"};
  public JMXServiceURL serviceURL;
  public KafkaMetricClient kafkaMetricClient;
  public String nodeID;
  HashMap<String, Double> metricsValues;
  Collection<String> argumentTargetMetrics;

  NodeMetrics(String ID, String address) throws MalformedURLException {
    nodeID = ID;
    serviceURL = new JMXServiceURL(createJmxUrl(address));
    kafkaMetricClient = new KafkaMetricClient(serviceURL);
    metricsValues = new HashMap();
    argumentTargetMetrics = List.of(metricsName).subList(0, metricsName.length);
  }

  public String createJmxUrl(String address) {
    return String.format(JMX_URI_FORMAT, address);
  }

  public void refreshMetrics() {
    Collection<BeanObject> metricsBean =
        argumentTargetMetrics.stream()
            .map(BrokerTopicMetrics::valueOf)
            .map(kafkaMetricClient::requestMetric)
            .map(BrokerTopicMetricsResult::beanObject)
            .collect(Collectors.toUnmodifiableList());

    for (BeanObject mb : metricsBean) {
      metricsValues.put(mb.domainName(), (Double) mb.getAttributes().get("MeanRate"));
    }
  }

  public double totalBytesPerSec() {
    return metricsValues.get("BytesInPerSec") + metricsValues.get("BytesOutPerSec");
  }
}
