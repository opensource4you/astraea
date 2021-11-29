package org.astraea.partitioner;

import org.astraea.Utils;
import org.astraea.metrics.BeanCollector;
import org.astraea.metrics.java.JvmMemory;
import org.astraea.metrics.jmx.MBeanClient;
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
          .nodes()
          .forEach(
              entry ->
                  Assertions.assertNotEquals(
                      0, collector.objects(entry.getKey(), entry.getValue()).size()));
    }
  }
}
