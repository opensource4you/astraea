package org.astraea.partitioner.nodeLoadMetric;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashMap;
import org.junit.jupiter.api.Test;

public class BrokersWeightTest {

  @Test
  public void testSetBrokerHashMap() {
    HashMap<Integer, Double> poissonMap = new HashMap<>();
    poissonMap.put(0, 0.5);
    poissonMap.put(1, 0.8);
    poissonMap.put(2, 0.3);

    BrokersWeight brokersWeight = new BrokersWeight();

    brokersWeight.setBrokerHashMap(poissonMap);

    assertEquals(brokersWeight.getBrokerHashMap().get(0)[0], 10);
    assertEquals(brokersWeight.getBrokerHashMap().get(1)[0], 3);

    brokersWeight.setBrokerHashMapValue(0, 8);
    brokersWeight.setBrokerHashMap(poissonMap);
    assertEquals(brokersWeight.getBrokerHashMap().get(0)[1], 8);
  }
}
