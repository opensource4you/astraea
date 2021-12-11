package org.astraea.partitioner.nodeLoadMetric;

import static org.mockito.Mockito.mockConstruction;

import org.astraea.metrics.BeanCollector;
import org.astraea.service.RequireBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;

public class BeanCollectorFactoryTest extends RequireBrokerCluster {
  @Test
  void testSingleton() {
    var FACTORY = new BeanCollectorFactory();
    try (MockedConstruction mocked = mockConstruction(BeanCollector.class)) {
      var bc1 = FACTORY.beanCollector();
      var bc2 = FACTORY.beanCollector();
      Assertions.assertEquals(bc1, bc2);
      var bc3 = FACTORY.beanCollector();
      var bc4 = FACTORY.beanCollector();
      Assertions.assertEquals(FACTORY.factoryCount(), 4);
      FACTORY.close();
      Assertions.assertEquals(FACTORY.factoryCount(), 3);
      FACTORY.close();
      FACTORY.close();
      Assertions.assertEquals(FACTORY.factoryCount(), 1);
      FACTORY.close();
      Assertions.assertEquals(FACTORY.factoryCount(), 0);
      var bc5 = FACTORY.beanCollector();
      Assertions.assertEquals(FACTORY.factoryCount(), 1);
      FACTORY.close();
      Assertions.assertEquals(FACTORY.factoryCount(), 0);
    }
  }
}
