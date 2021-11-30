package org.astraea.metrics;

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
      collector.addClient(
          jmxServiceURL().getHost(), jmxServiceURL().getPort(), KafkaMetrics.Host::jvmMemory);
      Utils.waitFor(() -> collector.size() > 0);
      collector
          .objects()
          .values()
          .forEach(
              all -> all.forEach(object -> Assertions.assertTrue(object instanceof JvmMemory)));

      collector
          .nodes()
          .forEach(
              node ->
                  Assertions.assertNotEquals(
                      0, collector.objects(node.host(), node.port()).size()));
    }
  }

  @Test
  void testAddDuplicateClient() throws Exception {
    try (var collector = new BeanCollector()) {
      collector.addClient(
          jmxServiceURL().getHost(), jmxServiceURL().getPort(), KafkaMetrics.Host::jvmMemory);
      collector.addClient(
          jmxServiceURL().getHost(), jmxServiceURL().getPort(), KafkaMetrics.Host::jvmMemory);
      collector.addClient(
          jmxServiceURL().getHost(), jmxServiceURL().getPort(), KafkaMetrics.Host::jvmMemory);

      Assertions.assertEquals(1, collector.nodes().size());
    }
  }
}
