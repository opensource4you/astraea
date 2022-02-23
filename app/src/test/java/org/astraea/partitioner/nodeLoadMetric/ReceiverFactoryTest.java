package org.astraea.partitioner.nodeLoadMetric;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;
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
    FACTORY.close(jmxAddress);
    Assertions.assertEquals(FACTORY.factoryCount(jmxAddress), 3);
    FACTORY.close(jmxAddress);
    FACTORY.close(jmxAddress);
    Assertions.assertEquals(FACTORY.factoryCount(jmxAddress), 1);
    FACTORY.close(jmxAddress);
    Assertions.assertEquals(FACTORY.factoryCount(jmxAddress), 0);
    var bc5 = FACTORY.receiversList(jmxAddress);
    Assertions.assertEquals(FACTORY.factoryCount(jmxAddress), 1);
    FACTORY.close(jmxAddress);
    Assertions.assertEquals(FACTORY.factoryCount(jmxAddress), 0);
  }

  @Test
  void test() throws IOException {
    var jmxAddress = Map.of(jmxServiceURL().getHost(), jmxServiceURL().getPort());
    var FACTORY = new ReceiverFactory();
    var bc1 = FACTORY.receiversList(jmxAddress);

    var c0 = bc1.get(0).current();
    var c1 = bc1.get(0).current();
    Assertions.assertTrue(c1.contains(c0.get(0)));
    sleep(1);
    var c2 = bc1.get(0).current();
    Assertions.assertFalse(c2.contains(c0.get(0)));
  }

  private static void sleep(int seconds) {
    try {
      TimeUnit.SECONDS.sleep(seconds);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}
