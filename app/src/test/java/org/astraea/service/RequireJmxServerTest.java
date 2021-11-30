package org.astraea.service;

import org.astraea.metrics.jmx.BeanQuery;
import org.astraea.metrics.jmx.MBeanClient;
import org.astraea.metrics.kafka.KafkaMetrics;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class RequireJmxServerTest extends RequireBrokerCluster {

  @Test
  void testQueryBeans() {
    testQueryBeans(MBeanClient.jndi(jmxServiceURL().getHost(), jmxServiceURL().getPort()));
    testQueryBeans(MBeanClient.of(jmxServiceURL()));
  }

  private void testQueryBeans(MBeanClient client) {
    try (client) {
      var result = client.queryBeans(BeanQuery.all());
      Assertions.assertFalse(result.isEmpty());
    }
  }

  @Test
  void testMemory() throws Exception {
    testMemory(MBeanClient.jndi(jmxServiceURL().getHost(), jmxServiceURL().getPort()));
    testMemory(MBeanClient.of(jmxServiceURL()));
  }

  private void testMemory(MBeanClient client) {
    try (client) {
      var memory = KafkaMetrics.Host.jvmMemory(client);
      Assertions.assertNotEquals(0, memory.heapMemoryUsage().getMax());
    }
  }
}
