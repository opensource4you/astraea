package org.astraea.partitioner.nodeLoadMetric;

import org.junit.jupiter.api.Test;

import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestBrokersWeight {
    LoadPoisson loadPoisson = mock(LoadPoisson.class);
    @Test
    public void testSetBrokerHashMap() {
        HashMap<Integer, Double> poissonMap = new HashMap<>();
        poissonMap.put(0, 0.5);
        poissonMap.put(1, 0.8);
        poissonMap.put(2, 0.3);

        when(loadPoisson.setAllPoisson()).thenReturn(poissonMap);

        BrokersWeight brokersWeight = new BrokersWeight(loadPoisson);

        brokersWeight.setBrokerHashMap();

        assertEquals(brokersWeight.getBrokerHashMap().get(0)[0], 10);
        assertEquals(brokersWeight.getBrokerHashMap().get(1)[0], 3);

        brokersWeight.setBrokerHashMapValue(0,8);
        brokersWeight.setBrokerHashMap();
        assertEquals(brokersWeight.getBrokerHashMap().get(0)[1], 8);
    }
}
