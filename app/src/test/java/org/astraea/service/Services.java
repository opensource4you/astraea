package org.astraea.service;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import kafka.server.KafkaConfig;
import kafka.server.KafkaConfig$;
import kafka.server.KafkaServer;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.astraea.Utils;

public final class Services {

  private Services() {}

  static BrokerCluster brokerCluster(ZookeeperCluster zk, int numberOfBrokers) {
    Map<Integer, Set<File>> tempFolders = new HashMap<>();
    for (var b = 0; b <= numberOfBrokers; b++) {
      tempFolders.put(
          b,
          Set.of(
              Utils.createTempDirectory("local_kafka"), Utils.createTempDirectory("local_kafka")));
    }

    var brokers =
        IntStream.range(0, numberOfBrokers)
            .mapToObj(
                index -> {
                  var path =
                      tempFolders.get(index).stream()
                          .map(String::valueOf)
                          .collect(Collectors.joining(","));
                  Properties config = new Properties();
                  // reduce the number from partitions and replicas to speedup the mini cluster
                  config.setProperty(
                      KafkaConfig$.MODULE$.OffsetsTopicPartitionsProp(), String.valueOf(1));
                  config.setProperty(
                      KafkaConfig$.MODULE$.OffsetsTopicReplicationFactorProp(), String.valueOf(1));
                  config.setProperty(KafkaConfig$.MODULE$.ZkConnectProp(), zk.connectionProps());
                  config.setProperty(KafkaConfig$.MODULE$.BrokerIdProp(), String.valueOf(index));
                  // bind broker on random port
                  config.setProperty(KafkaConfig$.MODULE$.ListenersProp(), "PLAINTEXT://:0");
                  config.setProperty(KafkaConfig$.MODULE$.LogDirProp(), path);
                  // increase the timeout in order to avoid ZkTimeoutException
                  config.setProperty(
                      KafkaConfig$.MODULE$.ZkSessionTimeoutMsProp(), String.valueOf(30 * 1000));
                  KafkaServer broker =
                      new KafkaServer(
                          new KafkaConfig(config), SystemTime.SYSTEM, scala.Option.empty(), false);

                  broker.startup();
                  return broker;
                })
            .collect(Collectors.toUnmodifiableList());
    String connectionProps =
        brokers.stream()
            .map(broker -> Utils.hostname() + ":" + broker.boundPort(new ListenerName("PLAINTEXT")))
            .collect(Collectors.joining(","));
    return new BrokerCluster() {

      @Override
      public void close() {
        brokers.forEach(
            broker -> {
              broker.shutdown();
              broker.awaitShutdown();
            });
        for (var b : tempFolders.keySet()) tempFolders.get(b).forEach(Utils::delete);
      }

      @Override
      public String bootstrapServers() {
        return connectionProps;
      }

      @Override
      public Map<Integer, Set<String>> logFolders() {
        return IntStream.range(0, numberOfBrokers)
            .mapToObj(brokerId -> Map.entry(brokerId, Set.of(tempFolders.get(brokerId).toString())))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
      }
    };
  }

  static ZookeeperCluster zookeeperCluster() {
    final NIOServerCnxnFactory factory;
    var snapshotDir = Utils.createTempDirectory("local_zk_snapshot");
    var logDir = Utils.createTempDirectory("local_zk_log");

    try {
      factory = new NIOServerCnxnFactory();
      factory.configure(new InetSocketAddress("0.0.0.0", 0), 1024);
      factory.startup(new ZooKeeperServer(snapshotDir, logDir, 500));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    return new ZookeeperCluster() {
      @Override
      public void close() {
        factory.shutdown();
        Utils.delete(snapshotDir);
        Utils.delete(logDir);
      }

      @Override
      public String connectionProps() {
        return Utils.hostname() + ":" + factory.getLocalPort();
      }
    };
  }
}
