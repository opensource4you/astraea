package org.astraea.cost;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.apache.kafka.common.Cluster;
import org.astraea.admin.Admin;
import org.astraea.metrics.HasBeanObject;
import org.astraea.service.RequireBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class ClusterInfoTest extends RequireBrokerCluster {

  @Test
  void testNode() {
    var node = NodeInfoTest.node();
    var partition = ReplicaInfoTest.partitionInfo();
    var kafkaCluster = Mockito.mock(Cluster.class);
    Mockito.when(kafkaCluster.availablePartitionsForTopic(partition.topic()))
        .thenReturn(List.of(partition));
    Mockito.when(kafkaCluster.partitionsForTopic(partition.topic())).thenReturn(List.of(partition));
    Mockito.when(kafkaCluster.nodes()).thenReturn(List.of(node));

    var clusterInfo = ClusterInfo.of(kafkaCluster);

    Assertions.assertEquals(1, clusterInfo.nodes().size());
    Assertions.assertEquals(NodeInfo.of(node), clusterInfo.nodes().get(0));
    Assertions.assertEquals(clusterInfo.nodes().get(0), clusterInfo.node(node.host(), node.port()));
    Assertions.assertEquals(1, clusterInfo.availableReplicas(partition.topic()).size());
    Assertions.assertEquals(1, clusterInfo.replicas(partition.topic()).size());
    Assertions.assertEquals(
        NodeInfo.of(node), clusterInfo.availableReplicas(partition.topic()).get(0).nodeInfo());
    Assertions.assertEquals(
        NodeInfo.of(node),
        clusterInfo.availableReplicaLeaders(partition.topic()).get(0).nodeInfo());
    Assertions.assertEquals(
        NodeInfo.of(node), clusterInfo.replicas(partition.topic()).get(0).nodeInfo());
  }

  @Test
  void testClusterInfoFromAdmin() {
    try (Admin admin = Admin.of(bootstrapServers())) {
      Supplier<Integer> randomNumber =
          () -> ThreadLocalRandom.current().nextInt(0, Integer.MAX_VALUE);
      String topic0 = "testClusterInfoFromAdmin_" + randomNumber.get();
      String topic1 = "testClusterInfoFromAdmin_" + randomNumber.get();
      String topic2 = "testClusterInfoFromAdmin_" + randomNumber.get();
      String excludedTopic = "testClusterInfoFromAdmin_excluded_" + randomNumber.get();
      int partitionCount = 10;
      short replicaCount = 2;

      Stream.of(topic0, topic1, topic2, excludedTopic)
          .forEach(
              topicName ->
                  admin
                      .creator()
                      .topic(topicName)
                      .numberOfPartitions(partitionCount)
                      .numberOfReplicas(replicaCount)
                      .create());

      final var topicPattern = Pattern.compile("^testClusterInfoFromAdmin_\\d{0,13}$");
      final var clusterInfo = ClusterInfo.of(admin, topicPattern.asMatchPredicate());

      Assertions.assertTrue(topicPattern.matcher(topic0).matches());
      Assertions.assertTrue(topicPattern.matcher(topic1).matches());
      Assertions.assertTrue(topicPattern.matcher(topic2).matches());
      Assertions.assertEquals(brokerIds().size(), clusterInfo.nodes().size());
      Assertions.assertEquals(Set.of(topic0, topic1, topic2), clusterInfo.topics());
      Assertions.assertEquals(partitionCount * replicaCount, clusterInfo.replicas(topic0).size());
      Assertions.assertEquals(partitionCount * replicaCount, clusterInfo.replicas(topic1).size());
      Assertions.assertEquals(partitionCount * replicaCount, clusterInfo.replicas(topic2).size());
      brokerIds()
          .forEach(
              id -> Assertions.assertEquals(logFolders().get(id), clusterInfo.dataDirectories(id)));
    }
  }

  @Test
  void testEmptyBeans() {
    var clusterInfo = ClusterInfo.of(Mockito.mock(org.apache.kafka.common.Cluster.class));
    Assertions.assertEquals(0, clusterInfo.allBeans().size());
    Assertions.assertEquals(0, clusterInfo.beans(19).size());
  }

  @Test
  void testBeans() {
    var beans = Map.of(1, (Collection<HasBeanObject>) List.of(Mockito.mock(HasBeanObject.class)));
    var origin = Mockito.mock(ClusterInfo.class);
    Mockito.when(origin.allBeans()).thenReturn(Map.of());
    var clusterInfo = ClusterInfo.of(origin, beans);
    Assertions.assertEquals(1, clusterInfo.allBeans().size());
    Assertions.assertEquals(0, clusterInfo.beans(19).size());
    Assertions.assertEquals(1, clusterInfo.beans(1).size());
  }
}
