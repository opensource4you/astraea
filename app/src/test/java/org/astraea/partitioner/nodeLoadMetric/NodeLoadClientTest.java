package org.astraea.partitioner.nodeLoadMetric;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class NodeLoadClientTest {
    @Test
    public void testGetBinOneCount() {
        NodeLoadClient nodeLoadClient = new NodeLoadClient();
        assertEquals(nodeLoadClient.getBinOneCount(7), 3);
    }
}
