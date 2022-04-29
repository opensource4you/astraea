package org.astraea.cost;

import org.apache.kafka.common.Node;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ReplicaInfoTest {

  @Test
  void testReplicaInfoOf0() {
    var node = new Node(43, "host", 100);
    var nodeInfo = NodeInfo.of(node);
    var replicaInfo = ReplicaInfo.of(nodeInfo);

    Assertions.assertEquals(replicaInfo.nodeInfo(), nodeInfo);
    Assertions.assertFalse(replicaInfo.dataFolder().isPresent());
  }

  @Test
  void testReplicaInfoOf1() {
    var node = new Node(43, "host", 100);
    var nodeInfo = NodeInfo.of(node);
    var dataFolder = "/path/to/somewhere";
    var replicaInfo = ReplicaInfo.of(nodeInfo, dataFolder);

    Assertions.assertEquals(replicaInfo.nodeInfo(), nodeInfo);
    Assertions.assertTrue(replicaInfo.dataFolder().isPresent());
    Assertions.assertEquals(replicaInfo.dataFolder().get(), dataFolder);
  }

  private ReplicaInfo create(int brokerId, String hostname, int port, String dataFolder) {
    var node = new Node(brokerId, hostname, port);
    var nodeInfo = NodeInfo.of(node);
    return ReplicaInfo.of(nodeInfo, dataFolder);
  }

  @Test
  void testEquals() {
    // different data folder
    var replicaInfo0 = create(1001, "host0", 9092, "/path/to/somewhere");
    var replicaInfo1 = create(1001, "host0", 9092, "/path/to/nowhere");
    Assertions.assertNotEquals(replicaInfo0, replicaInfo1);

    // different node(broker id)
    var replicaInfo2 = create(1001, "host0", 9092, "/path/to/somewhere");
    var replicaInfo3 = create(1002, "host0", 9092, "/path/to/somewhere");
    Assertions.assertNotEquals(replicaInfo2, replicaInfo3);

    // compare with null raise no error
    var replicaInfo6 = create(1001, "host0", 9092, "/path/to/somewhere");
    var replicaInfo7 = create(1001, "host0", 9092, "/path/to/somewhere");
    Assertions.assertNotEquals(replicaInfo6, null);
    Assertions.assertNotEquals(null, replicaInfo7);

    // will equal
    var replicaInfo8 = create(1001, "host0", 9092, "/path/to/somewhere");
    var replicaInfo9 = create(1001, "host0", 9092, "/path/to/somewhere");
    Assertions.assertEquals(replicaInfo8, replicaInfo9);

    // no data folder
    var replicaInfo10 = create(1001, "host0", 9092, null);
    var replicaInfo11 = create(1001, "host0", 9092, null);
    Assertions.assertEquals(replicaInfo10, replicaInfo11);

    // one has the data folder
    var replicaInfo12 = create(1001, "host0", 9092, "/path/to/nowhere");
    var replicaInfo13 = create(1001, "host0", 9092, null);
    Assertions.assertNotEquals(replicaInfo12, replicaInfo13);
    Assertions.assertNotEquals(replicaInfo13, replicaInfo12);
  }

  @Test
  void testCompareAndError() {
    var replicaInfo0 = create(1001, "host0", 9092, "/path/to/somewhere");
    var replicaInfo1 = create(1001, "host0", 9092, null);

    //noinspection ConstantConditions,ResultOfMethodCallIgnored
    Assertions.assertThrows(NullPointerException.class, () -> replicaInfo0.compareTo(null));
    Assertions.assertDoesNotThrow(() -> replicaInfo0.compareTo(replicaInfo1));
    Assertions.assertDoesNotThrow(() -> replicaInfo1.compareTo(replicaInfo0));
  }
}
