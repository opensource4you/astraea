package org.astraea.partitioner.smooth;

import java.util.List;
import java.util.Map;
import org.astraea.cost.ClusterInfo;
import org.astraea.cost.FakeClusterInfo;
import org.astraea.cost.NodeInfo;
import org.astraea.cost.ReplicaInfo;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class SmoothWeightRoundRobinTest {
  @Test
  void testGetAndChoose() {
    var topic = "test";
    var smoothWeight = new SmoothWeightRoundRobin(Map.of(1, 5.0, 2, 3.0, 3, 1.0));
    var testCluster = mockResult();

    Assertions.assertEquals(1, smoothWeight.getAndChoose(topic, testCluster));
    Assertions.assertEquals(2, smoothWeight.getAndChoose(topic, testCluster));
    Assertions.assertEquals(3, smoothWeight.getAndChoose(topic, testCluster));
    Assertions.assertEquals(1, smoothWeight.getAndChoose(topic, testCluster));
    Assertions.assertEquals(2, smoothWeight.getAndChoose(topic, testCluster));
    Assertions.assertEquals(3, smoothWeight.getAndChoose(topic, testCluster));
    Assertions.assertEquals(1, smoothWeight.getAndChoose(topic, testCluster));
  }

  @Test
  void testPartOfBrokerGetAndChoose() {
    var topic = "test";
    var smoothWeight = new SmoothWeightRoundRobin(Map.of(1, 5.0, 2, 3.0, 3, 1.0, 4, 1.0));
    var testCluster = mockResult();

    Assertions.assertEquals(1, smoothWeight.getAndChoose(topic, testCluster));
    Assertions.assertEquals(2, smoothWeight.getAndChoose(topic, testCluster));
    Assertions.assertEquals(3, smoothWeight.getAndChoose(topic, testCluster));
    Assertions.assertEquals(1, smoothWeight.getAndChoose(topic, testCluster));
    Assertions.assertEquals(2, smoothWeight.getAndChoose(topic, testCluster));
    Assertions.assertEquals(3, smoothWeight.getAndChoose(topic, testCluster));
    Assertions.assertEquals(1, smoothWeight.getAndChoose(topic, testCluster));
  }

  ClusterInfo mockResult() {
    var re1 = Mockito.mock(ReplicaInfo.class);
    var node1 = Mockito.mock(NodeInfo.class);
    Mockito.when(re1.nodeInfo()).thenReturn(node1);
    Mockito.when(node1.id()).thenReturn(1);

    var re2 = Mockito.mock(ReplicaInfo.class);
    var node2 = Mockito.mock(NodeInfo.class);
    Mockito.when(re2.nodeInfo()).thenReturn(node2);
    Mockito.when(node2.id()).thenReturn(2);

    var re3 = Mockito.mock(ReplicaInfo.class);
    var node3 = Mockito.mock(NodeInfo.class);
    Mockito.when(re3.nodeInfo()).thenReturn(node3);
    Mockito.when(node3.id()).thenReturn(3);
    return new FakeClusterInfo() {
      @Override
      public List<ReplicaInfo> availableReplicaLeaders(String topic) {
        return List.of(re1, re2, re3);
      }
    };
  }
}
