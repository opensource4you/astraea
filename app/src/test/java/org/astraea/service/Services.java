package org.astraea.service;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import kafka.server.KafkaConfig;
import kafka.server.KafkaConfig$;
import kafka.server.KafkaServer;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.astraea.common.Utils;

public final class Services {

  private Services() {}

  static BrokerCluster brokerCluster(ZookeeperCluster zk, int numberOfBrokers) {
    var tempFolders =
        IntStream.range(0, numberOfBrokers)
            .boxed()
            .collect(
                Collectors.toMap(
                    Function.identity(),
                    brokerId ->
                        Set.of(
                            Utils.createTempDirectory("local_kafka").getAbsolutePath(),
                            Utils.createTempDirectory("local_kafka").getAbsolutePath(),
                            Utils.createTempDirectory("local_kafka").getAbsolutePath())));

    var brokers =
        IntStream.range(0, numberOfBrokers)
            .mapToObj(
                index -> {
                  Properties config = new Properties();
                  // reduce the backoff of compact thread to test it quickly
                  config.setProperty(
                      KafkaConfig$.MODULE$.LogCleanerBackoffMsProp(), String.valueOf(2000));
                  // reduce the number from partitions and replicas to speedup the mini cluster
                  config.setProperty(
                      KafkaConfig$.MODULE$.OffsetsTopicPartitionsProp(), String.valueOf(1));
                  config.setProperty(
                      KafkaConfig$.MODULE$.OffsetsTopicReplicationFactorProp(), String.valueOf(1));
                  config.setProperty(KafkaConfig$.MODULE$.ZkConnectProp(), zk.connectionProps());
                  config.setProperty(KafkaConfig$.MODULE$.BrokerIdProp(), String.valueOf(index));
                  // bind broker on random port
                  config.setProperty(KafkaConfig$.MODULE$.ListenersProp(), "PLAINTEXT://:0");
                  config.setProperty(
                      KafkaConfig$.MODULE$.LogDirsProp(), String.join(",", tempFolders.get(index)));

                  // increase the timeout in order to avoid ZkTimeoutException
                  config.setProperty(
                      KafkaConfig$.MODULE$.ZkSessionTimeoutMsProp(), String.valueOf(30 * 1000));
                  KafkaServer broker =
                      new KafkaServer(
                          new KafkaConfig(config), SystemTime.SYSTEM, scala.Option.empty(), false);
                  broker.startup();
                  return Map.entry(index, broker);
                })
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    AtomicReference<String> connectionProps = new AtomicReference<>();
    connectionProps.set(
        brokers.values().stream()
            .map(
                kafkaServer ->
                    Utils.hostname() + ":" + kafkaServer.boundPort(new ListenerName("PLAINTEXT")))
            .collect(Collectors.joining(",")));
    return new BrokerCluster() {
      @Override
      public void close(int brokerID) {
        var broker = brokers.remove(brokerID);
        if (broker != null) {
          broker.shutdown();
          broker.awaitShutdown();
          var folders = tempFolders.remove(brokerID);
          if (folders != null) folders.forEach(f -> Utils.delete(new File(f)));
          connectionProps.set(
              brokers.values().stream()
                  .map(
                      kafkaServer ->
                          Utils.hostname()
                              + ":"
                              + kafkaServer.boundPort(new ListenerName("PLAINTEXT")))
                  .collect(Collectors.joining(",")));
        }
      }

      @Override
      public void close() {
        for (int i = 0; i < brokers.size(); i++) close(i);
      }

      @Override
      public String bootstrapServers() {
        return connectionProps.get();
      }

      @Override
      public Map<Integer, Set<String>> logFolders() {
        return IntStream.range(0, numberOfBrokers)
            .boxed()
            .filter(tempFolders::containsKey)
            .collect(Collectors.toMap(Function.identity(), tempFolders::get));
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
