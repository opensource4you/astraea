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
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.utils.SystemTime;

public interface BrokerCluster extends AutoCloseable {

  static BrokerCluster of(
      ZookeeperCluster zookeeperCluster, int numberOfBrokers, Map<String, String> override) {
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
                  var configs = new HashMap<String, String>();
                  // reduce the backoff of compact thread to test it quickly
                  configs.put("log.cleaner.backoff.ms", String.valueOf(2000));
                  // reduce the number from partitions and replicas to speedup the mini cluster
                  configs.put("offsets.topic.num.partitions", String.valueOf(1));
                  configs.put("offsets.topic.replication.factor", String.valueOf(1));
                  configs.put("zookeeper.connect", zookeeperCluster.connectionProps());
                  configs.put("broker.id", String.valueOf(index));
                  // bind broker on random port
                  configs.put("listeners", "PLAINTEXT://:0");
                  configs.put("log.dirs", String.join(",", tempFolders.get(index)));

                  // TODO: provide a mechanism to offer customized embedded cluster for specialized
                  // test scenario. keeping adding config to this method might cause configuration
                  // requirement to conflict. See https://github.com/skiptests/astraea/issues/391
                  // for further discussion.

                  // disable auto leader balance to ensure AdminTest#preferredLeaderElection works
                  // correctly.
                  configs.put("auto.leader.rebalance.enable", String.valueOf(false));

                  // increase the timeout in order to avoid ZkTimeoutException
                  configs.put("zookeeper.session.timeout.ms", String.valueOf(30 * 1000));

                  // add custom configs
                  configs.putAll(override);

                  KafkaServer broker =
                      new KafkaServer(
                          new KafkaConfig(configs), SystemTime.SYSTEM, scala.Option.empty(), false);
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
        Set.copyOf(brokers.keySet()).forEach(this::close);
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

  /**
   * @return brokers information. the form is "host_a:port_a,host_b:port_b"
   */
  String bootstrapServers();

  /**
   * @return the log folders used by each broker
   */
  Map<Integer, Set<String>> dataFolders();

  /**
   * @param brokerID the broker id want to close
   */
  void close(int brokerID);

  @Override
  void close();
}
