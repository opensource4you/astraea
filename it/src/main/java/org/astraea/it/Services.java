/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.astraea.it;

import java.io.File;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
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
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.connect.cli.ConnectDistributed;
import org.apache.kafka.connect.runtime.Connect;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.distributed.DistributedConfig;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;

public final class Services {

  private Services() {}

  static WorkerCluster workerCluster(BrokerCluster bk, int[] ports) {
    List<Connect> connects =
        Arrays.stream(ports)
            .mapToObj(
                port -> {
                  var realPort = Utils.resolvePort(port);
                  Map<String, String> config = new HashMap<>();
                  // reduce the number from partitions and replicas to speedup the mini cluster
                  // for setting storage. the partition from setting topic is always 1 so we
                  // needn't to set it to 1 here.
                  config.put(DistributedConfig.CONFIG_TOPIC_CONFIG, "connect-config");
                  config.put(DistributedConfig.CONFIG_STORAGE_REPLICATION_FACTOR_CONFIG, "1");
                  // for offset storage
                  config.put(DistributedConfig.OFFSET_STORAGE_TOPIC_CONFIG, "connect-offsets");
                  config.put(DistributedConfig.OFFSET_STORAGE_PARTITIONS_CONFIG, "1");
                  config.put(DistributedConfig.OFFSET_STORAGE_REPLICATION_FACTOR_CONFIG, "1");
                  // for status storage
                  config.put(DistributedConfig.STATUS_STORAGE_TOPIC_CONFIG, "connect-status");
                  config.put(DistributedConfig.STATUS_STORAGE_PARTITIONS_CONFIG, "1");
                  config.put(DistributedConfig.STATUS_STORAGE_REPLICATION_FACTOR_CONFIG, "1");
                  // set the brokers info
                  config.put(WorkerConfig.BOOTSTRAP_SERVERS_CONFIG, bk.bootstrapServers());
                  config.put(ConsumerConfig.GROUP_ID_CONFIG, "connect");
                  // set the normal converter
                  config.put(
                      ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG,
                      "org.apache.kafka.connect.converters.ByteArrayConverter");
                  config.put(
                      ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG,
                      "org.apache.kafka.connect.converters.ByteArrayConverter");
                  config.put(
                      WorkerConfig.LISTENERS_CONFIG,
                      // the worker hostname is a part of information used by restful apis.
                      // the 0.0.0.0 make all connector say that they are executed by 0.0.0.0
                      // and it does make sense in production. With a view to testing the
                      // related codes in other modules, we have to define the "really" hostname
                      // in starting worker cluster.
                      "http://" + Utils.hostname() + ":" + realPort);
                  config.put(WorkerConfig.OFFSET_COMMIT_INTERVAL_MS_CONFIG, String.valueOf(500));
                  // enable us to override the connector configs
                  config.put(WorkerConfig.CONNECTOR_CLIENT_POLICY_CLASS_CONFIG, "All");
                  return new ConnectDistributed().startConnect(config);
                })
            .collect(Collectors.toUnmodifiableList());

    return new WorkerCluster() {
      @Override
      public List<URL> workerUrls() {
        return connects.stream()
            .map(Connect::restUrl)
            .map(
                uri -> {
                  try {
                    return uri.toURL();
                  } catch (MalformedURLException e) {
                    throw new RuntimeException(e);
                  }
                })
            .collect(Collectors.toList());
      }

      @Override
      public void close() {
        connects.forEach(
            connect -> {
              connect.stop();
              connect.awaitStop();
            });
      }
    };
  }

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

                  // TODO: provide a mechanism to offer customized embedded cluster for specialized
                  // test scenario. keeping adding config to this method might cause configuration
                  // requirement to conflict. See https://github.com/skiptests/astraea/issues/391
                  // for further discussion.

                  // disable auto leader balance to ensure AdminTest#preferredLeaderElection works
                  // correctly.
                  config.setProperty(
                      KafkaConfig$.MODULE$.AutoLeaderRebalanceEnableProp(), String.valueOf(false));

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
        IntStream.range(0, brokers.size() - 1).forEach(this::close);
      }

      @Override
      public String bootstrapServers() {
        return connectionProps.get();
      }

      @Override
      public Map<Integer, Set<String>> dataFolders() {
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
