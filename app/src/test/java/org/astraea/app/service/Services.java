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
package org.astraea.app.service;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
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
import org.astraea.app.common.Utils;

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
        tempFolders.values().forEach(fs -> fs.forEach(f -> Utils.delete(new File(f))));
      }

      @Override
      public String bootstrapServers() {
        return connectionProps;
      }

      @Override
      public Map<Integer, Set<String>> logFolders() {
        return IntStream.range(0, numberOfBrokers)
            .boxed()
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
