package org.astraea.service;

import java.util.Set;
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

  @Test
  void testHost() {
    var legalChars = Set.of('0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '.');
    var address = jmxServiceURL().getHost();
    Assertions.assertNotEquals("127.0.0.1", address);
    Assertions.assertNotEquals("0.0.0.0", address);
    for (var i = 0; i < address.length(); i++) {
      Assertions.assertTrue(legalChars.contains(address.charAt(i)));
    }
  }
}
