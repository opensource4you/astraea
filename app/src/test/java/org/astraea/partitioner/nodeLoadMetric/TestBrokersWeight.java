package org.astraea.partitioner.nodeLoadMetric;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import org.junit.jupiter.api.Test;

public class TestBrokersWeight {
  LoadPoisson loadPoisson = mock(LoadPoisson.class);

  @Test
  public void testSetBrokerHashMap() {
    HashMap<String, Double> poissonMap = new HashMap<>();
    poissonMap.put("0", 0.5);
    poissonMap.put("1", 0.8);
    poissonMap.put("2", 0.3);

    when(loadPoisson.getAllPoissonMap()).thenReturn(poissonMap);

    BrokersWeight brokersWeight = new BrokersWeight(loadPoisson);

    brokersWeight.setBrokerWeightHashMap();

    assertEquals(brokersWeight.getBrokerHashMap().get("0")[0], 10);
    assertEquals(brokersWeight.getBrokerHashMap().get("1")[0], 3);

    brokersWeight.setBrokerHashMapValue("0", 8);
    brokersWeight.setBrokerWeightHashMap();
    assertEquals(brokersWeight.getBrokerHashMap().get("0")[1], 8);
  }
}
