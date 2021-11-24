package org.astraea.topic;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.apache.kafka.common.TopicPartition;
import org.astraea.argument.ArgumentUtil;
import org.astraea.producer.Producer;
import org.astraea.producer.Sender;
import org.astraea.service.RequireBrokerCluster;
import org.junit.jupiter.api.Test;

class ReplicaSyncingMonitorIntegrationTest extends RequireBrokerCluster {

  private static final String topicName =
      ReplicaSyncingMonitorIntegrationTest.class.getSimpleName();
  private static final byte[] dummyBytes = new byte[1024];

  @Test
  void execute() throws IOException, InterruptedException {
    // arrange
    try (TopicAdmin topicAdmin = TopicAdmin.of(bootstrapServers())) {
      topicAdmin
          .creator()
          .topic(topicName)
          .numberOfPartitions(1)
          .numberOfReplicas((short) 1)
          .create();

      Sender<byte[], byte[]> sender =
          Producer.of(bootstrapServers()).sender().topic(topicName).partition(0).value(dummyBytes);

      // create 16MB of data
      IntStream.range(0, 16 * 1024)
          .mapToObj(i -> sender.run().toCompletableFuture())
          .forEach(CompletableFuture::join);

      int currentBroker =
          topicAdmin
              .replicas(Set.of(topicName))
              .get(new TopicPartition(topicName, 0))
              .get(0)
              .broker();
      int moveToBroker = (currentBroker + 1) % logFolders().size();

      Thread executionThread =
          new Thread(
              () -> {
                topicAdmin.migrator().partition(topicName, 0).moveTo(Set.of(moveToBroker));
                ReplicaSyncingMonitor.execute(
                    topicAdmin,
                    ArgumentUtil.parseArgument(
                        new ReplicaSyncingMonitor.Argument(),
                        new String[] {
                          "--bootstrap.servers",
                          bootstrapServers(),
                          "--topic",
                          topicName,
                          "--interval",
                          "0.1"
                        }));
              });

      // act
      executionThread.start();
      TimeUnit.SECONDS.timedJoin(executionThread, 8); // wait until the thread exit
      TimeUnit.SECONDS.sleep(2); // sleep 2 extra seconds to ensure test run in stable

      // assert
      assertSame(Thread.State.TERMINATED, executionThread.getState());
      assertEquals(
          1, topicAdmin.replicas(Set.of(topicName)).get(new TopicPartition(topicName, 0)).size());
      assertEquals(
          moveToBroker,
          topicAdmin.replicas(Set.of(topicName)).get(new TopicPartition(topicName, 0)).stream()
              .filter(Replica::leader)
              .findFirst()
              .orElseThrow()
              .broker());
    }
  }
}
