package org.astraea.metrics.jmx;

import java.util.Map;
import java.util.stream.Collectors;
import org.astraea.Utils;
import org.astraea.metrics.java.JvmMemory;
import org.astraea.metrics.kafka.KafkaMetrics;
import org.astraea.service.RequireBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class BeanCollectorTest extends RequireBrokerCluster {

  @Test
  void testAddClient() throws Exception {
    try (var collector = new BeanCollector()) {
      collector.addClient(new MBeanClient(jmxServiceURL()), KafkaMetrics.Host::jvmMemory);
      Utils.waitFor(() -> collector.size() > 0);
      collector
          .objects()
          .values()
          .forEach(
              all -> all.forEach(object -> Assertions.assertTrue(object instanceof JvmMemory)));

      collector
          .nodesID()
          .forEach(
              n ->
                  Assertions.assertNotEquals(
                      0, collector.objects(n.getKey(), n.getValue()).size()));
    }
  }

  @Test
  void testNodesObjects() throws Exception {
    try (var collector = new BeanCollector()) {
      var mBean = new MBeanClient(jmxServiceURL());
      collector.addClient(mBean, KafkaMetrics.BrokerTopic.BytesInPerSec::fetch);
      collector.addClient(mBean, KafkaMetrics.BrokerTopic.BytesOutPerSec::fetch);
      Utils.waitFor(() -> collector.size() > 3);

      var metricsSize =
          collector.nodesObjects().values().stream()
              .map(Map::keySet)
              .collect(Collectors.toList())
              .get(0)
              .size();

      Assertions.assertEquals(metricsSize, 2);
    }
  }
}
