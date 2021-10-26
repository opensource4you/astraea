package org.astraea.service;

import org.astraea.metrics.jmx.BeanQuery;
import org.astraea.metrics.jmx.MBeanClient;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class RequireJmxServerTest extends RequireBrokerCluster {

  @Test
  void testJmxServer() throws Exception {
    try (var client = new MBeanClient(jmxServiceURL())) {
      var result = client.queryBeans(BeanQuery.all());
      Assertions.assertFalse(result.isEmpty());
    }
  }
}
