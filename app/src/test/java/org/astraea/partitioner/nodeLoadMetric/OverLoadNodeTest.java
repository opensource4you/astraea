package org.astraea.partitioner.nodeLoadMetric;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.HashMap;

public class OverLoadNodeTest {

    @Test
    public void testStandardDeviationImperative() {
        HashMap<Integer, Double> testHashMap = new HashMap<>();
        testHashMap.put(0,10.0);
        testHashMap.put(1,10.0);
        testHashMap.put(2,20.0);
        testHashMap.put(3,20.0);

        OverLoadNode overLoadNode = new OverLoadNode();
        overLoadNode.setEachBrokerMsgPerSec(testHashMap);
        overLoadNode.setAvgBrokersMsgPerSec();

        assertEquals(overLoadNode.getAvgBrokersMsgPerSec(), 15);

        overLoadNode.standardDeviationImperative();

        assertEquals(overLoadNode.getStandardDeviation(), 5 );
    }

    @Test
    public void testSetOverLoadCount() {
        OverLoadNode overLoadNode = new OverLoadNode();

        assertEquals(overLoadNode.setOverLoadCount(0, 2 ,1), 4);
        assertEquals(overLoadNode.setOverLoadCount(31, 2 ,1), 31);
        assertEquals(overLoadNode.setOverLoadCount(31, 2 ,0), 27);
        assertEquals(overLoadNode.setOverLoadCount(20, 4 ,0), 4);
    }

    @Test
    public void testMonitorOverLoad() {
        HashMap<Integer, Double> testHashMap = new HashMap<>();
        testHashMap.put(0,10.0);
        testHashMap.put(1,5.0);
        testHashMap.put(2,20.0);
        testHashMap.put(3,50.0);

        OverLoadNode overLoadNode = mock(OverLoadNode.class, withSettings()
                .useConstructor().defaultAnswer(CALLS_REAL_METHODS));
        HashMap<Integer, Integer> overLoadCountTenTimes = new HashMap<>();
        overLoadCountTenTimes.put(0,0);
        overLoadCountTenTimes.put(1,0);
        overLoadCountTenTimes.put(2,0);
        overLoadCountTenTimes.put(3,0);

        HashMap<Integer, Integer> overLoadCountTwentyTimes = new HashMap<>();
        overLoadCountTwentyTimes.put(0,0);
        overLoadCountTwentyTimes.put(1,0);
        overLoadCountTwentyTimes.put(2,0);
        overLoadCountTwentyTimes.put(3,0);

        doAnswer(invocation -> {
            overLoadNode.setEachBrokerMsgPerSec(testHashMap);
            return null;
        }).when(overLoadNode).setBrokersMsgPerSec();

        for (int i = 0; i < 20; i++){
            overLoadNode.setMountCount(i);
            overLoadNode.monitorOverLoad(overLoadCountTwentyTimes);
        }

        for (int i = 0; i < 10; i++){
            overLoadNode.setMountCount(i);
            overLoadNode.monitorOverLoad(overLoadCountTenTimes);
        }

        assertEquals(overLoadCountTwentyTimes.get(3), 1023);
        assertEquals(overLoadCountTenTimes, overLoadCountTwentyTimes);
    }

}
