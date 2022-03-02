package org.astraea.partitioner.nodeLoadMetric;

import java.util.Map;
import org.astraea.service.RequireBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ReceiverFactoryTest extends RequireBrokerCluster {
  @Test
  void testSingleton() {
    var jmxAddress = Map.of(jmxServiceURL().getHost(), jmxServiceURL().getPort());
    var FACTORY = new ReceiverFactory();
    var bc1 = FACTORY.receiversList(jmxAddress);
    var bc2 = FACTORY.receiversList(jmxAddress);
    Assertions.assertEquals(bc1, bc2);
    var bc3 = FACTORY.receiversList(jmxAddress);
    var bc4 = FACTORY.receiversList(jmxAddress);
    Assertions.assertEquals(FACTORY.factoryCount(jmxAddress), 4);
    FACTORY.close();
    Assertions.assertEquals(FACTORY.factoryCount(jmxAddress), 3);
    FACTORY.close();
    FACTORY.close();
    Assertions.assertEquals(FACTORY.factoryCount(jmxAddress), 1);
    FACTORY.close();
    Assertions.assertEquals(FACTORY.factoryCount(jmxAddress), 0);
    var bc5 = FACTORY.receiversList(jmxAddress);
    Assertions.assertEquals(FACTORY.factoryCount(jmxAddress), 1);
    FACTORY.close();
    Assertions.assertEquals(FACTORY.factoryCount(jmxAddress), 0);
  }
}
