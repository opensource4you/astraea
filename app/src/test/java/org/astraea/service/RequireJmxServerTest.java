package org.astraea.service;

import org.astraea.metrics.jmx.BeanQuery;
import org.astraea.metrics.jmx.MBeanClient;
import org.astraea.metrics.kafka.KafkaMetrics;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class RequireJmxServerTest extends RequireBrokerCluster {

  @Test
  void testQueryBeans() throws Exception {
    try (var client = new MBeanClient(jmxServiceURL())) {
      var result = client.queryBeans(BeanQuery.all());
      Assertions.assertFalse(result.isEmpty());
    }
  }

  @Test
  void testMemory() throws Exception {
    try (var client = new MBeanClient(jmxServiceURL())) {
      var memory = KafkaMetrics.Host.jvmMemory(client);
      Assertions.assertNotEquals(0, memory.heapMemoryUsage().getMax());
    }
  }
}
