package org.astraea.partitioner;

import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.astraea.cost.BrokerCost;
import org.astraea.cost.ClusterInfo;
import org.astraea.cost.HasBrokerCost;
import org.astraea.cost.NodeInfo;
import org.astraea.cost.PartitionInfo;
import org.astraea.metrics.collector.Fetcher;
import org.astraea.metrics.collector.Receiver;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class StrictCostDispatcherTest {

  private static PartitionInfo createFakePartitionInfo(
      String topic, int partition, NodeInfo leaderNode) {
    return PartitionInfo.of(topic, partition, leaderNode, null, null, null);
  }

  @Test
  void testClose() {
    var dispatcher = new StrictCostDispatcher();
    var receiver = Mockito.mock(Receiver.class);
    dispatcher.receivers.put(10, receiver);
    dispatcher.close();
    Mockito.verify(receiver, Mockito.times(1)).close();
    Assertions.assertEquals(0, dispatcher.receivers.size());
  }

  @Test
  void testConfigure() {
    try (var dispatcher = new StrictCostDispatcher()) {
      dispatcher.configure(Configuration.of(Map.of()));
      Assertions.assertThrows(NoSuchElementException.class, () -> dispatcher.jmxPort(0));
      dispatcher.configure(Configuration.of(Map.of(StrictCostDispatcher.JMX_PORT, "12345")));
      Assertions.assertEquals(12345, dispatcher.jmxPort(0));
    }
  }

  @Test
  void testBestPartition() {
    var partitions =
        List.of(
            createFakePartitionInfo("t", 0, NodeInfo.of(10, "h0", 1000)),
            createFakePartitionInfo("t", 1, NodeInfo.of(11, "h1", 1000)));

    var score = List.of(Map.of(10, 0.8D), Map.of(11, 0.7D));

    var result = StrictCostDispatcher.bestPartition(partitions, score).get();

    Assertions.assertEquals(0.7D, result.getValue());
    Assertions.assertEquals(1, result.getKey().partition());
  }

  @Test
  void testPartition() {
    // n0 is busy
    var n0 = NodeInfo.of(10, "host0", 12345);
    var p0 = createFakePartitionInfo("aa", 1, n0);
    // n1 is free
    var n1 = NodeInfo.of(11, "host1", 12345);
    var p1 = createFakePartitionInfo("aa", 2, n1);

    var receiver = Mockito.mock(Receiver.class);
    Mockito.when(receiver.current()).thenReturn(List.of());

    var costFunction =
        new HasBrokerCost() {
          @Override
          public BrokerCost brokerCost(ClusterInfo clusterInfo) {
            var partitionInfos = clusterInfo.availablePartitions("aa");
            var brokerCost =
                clusterInfo.allBeans().keySet().stream()
                    .collect(
                        Collectors.toMap(
                            Function.identity(), id -> id.equals(n0.id()) ? 0.9D : 0.5D));
            return () -> brokerCost;
          }

          @Override
          public Fetcher fetcher() {
            return client -> List.of();
          }
        };

    var dispatcher =
        new StrictCostDispatcher(List.of(costFunction)) {
          @Override
          Receiver receiver(String host, int port) {
            return receiver;
          }

          @Override
          int jmxPort(int id) {
            return 0;
          }
        };
    var clusterInfo = Mockito.mock(ClusterInfo.class);

    // there is no available partition
    Mockito.when(clusterInfo.availablePartitions("aa")).thenReturn(List.of());
    Mockito.when(clusterInfo.topics()).thenReturn(Set.of("aa"));
    Assertions.assertEquals(0, dispatcher.partition("aa", new byte[0], new byte[0], clusterInfo));

    // there is only one available partition
    Mockito.when(clusterInfo.availablePartitions("aa")).thenReturn(List.of(p0));
    Assertions.assertEquals(1, dispatcher.partition("aa", new byte[0], new byte[0], clusterInfo));

    // there is no beans, so it just returns the partition.
    Mockito.when(clusterInfo.availablePartitions("aa")).thenReturn(List.of(p0, p1));
    Assertions.assertEquals(2, dispatcher.partition("aa", new byte[0], new byte[0], clusterInfo));
  }
}
