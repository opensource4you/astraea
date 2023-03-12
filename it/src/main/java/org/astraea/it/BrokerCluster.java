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

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import kafka.server.KafkaConfig;
import kafka.server.KafkaRaftServer;
import kafka.server.MetaProperties;
import kafka.server.Server;
import kafka.tools.StorageTool;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.server.common.MetadataVersion;

public interface BrokerCluster extends AutoCloseable {

  private static CompletableFuture<Map.Entry<Integer, Server>> server(
      Map<String, String> configs, Set<String> folders, String clusterId, int nodeId) {
    StorageTool.formatCommand(
        new PrintStream(new ByteArrayOutputStream()),
        scala.collection.JavaConverters.collectionAsScalaIterableConverter(folders)
            .asScala()
            .toSeq(),
        new MetaProperties(clusterId, nodeId),
        MetadataVersion.latest(),
        true);

    return CompletableFuture.supplyAsync(
        () -> {
          var broker =
              new KafkaRaftServer(
                  new KafkaConfig(configs), SystemTime.SYSTEM, scala.Option.empty());
          broker.startup();
          return Map.entry(nodeId, broker);
        });
  }

  static BrokerCluster of(int numberOfBrokers, Map<String, String> override) {
    var tempFolders =
        IntStream.range(0, numberOfBrokers)
            .boxed()
            .collect(
                Collectors.toMap(
                    Function.identity(),
                    brokerId ->
                        Set.of(
                            Utils.createTempDirectory("local_kafka").toAbsolutePath().toString(),
                            Utils.createTempDirectory("local_kafka").toAbsolutePath().toString(),
                            Utils.createTempDirectory("local_kafka").toAbsolutePath().toString())));

    // get more available ports to avoid conflicts
    var ports =
        IntStream.range(0, numberOfBrokers * 2)
            .boxed()
            .map(ignored -> Utils.availablePort())
            .distinct()
            .limit(numberOfBrokers)
            .collect(Collectors.toUnmodifiableList());

    if (ports.size() != numberOfBrokers)
      throw new RuntimeException("failed to get enough available ports.");

    var availablePorts =
        IntStream.range(0, numberOfBrokers)
            .boxed()
            .collect(Collectors.toUnmodifiableMap(Function.identity(), ports::get));

    var clusterId = Uuid.randomUuid().toString();
    var controllerFolder = Utils.createTempDirectory("local_kafka").toAbsolutePath().toString();
    var controllerPort = Utils.availablePort();
    var voters = "999@localhost:" + controllerPort;
    var controller =
        server(
                Map.of(
                    "process.roles",
                    "controller",
                    "node.id",
                    "999",
                    "controller.quorum.voters",
                    voters,
                    "listeners",
                    "CONTROLLER://:" + controllerPort,
                    "controller.listener.names",
                    "CONTROLLER",
                    "listener.security.protocol.map",
                    "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
                    "log.dirs",
                    controllerFolder,
                    "num.partitions",
                    "1"),
                Set.of(controllerFolder),
                clusterId,
                999)
            .join()
            .getValue();

    var brokersFutures =
        availablePorts.entrySet().stream()
            .map(
                entry -> {
                  var configs = new HashMap<String, String>();
                  configs.put("process.roles", "broker");
                  configs.put("node.id", String.valueOf(entry.getKey()));
                  configs.put("controller.quorum.voters", voters);
                  configs.put("listeners", "PLAINTEXT://:" + entry.getValue());
                  configs.put("controller.listener.names", "CONTROLLER");
                  configs.put(
                      "listener.security.protocol.map", "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT");
                  // reduce the backoff of compact thread to test it quickly
                  configs.put("log.cleaner.backoff.ms", String.valueOf(2000));
                  // reduce the number from partitions and replicas to speedup the mini cluster
                  configs.put("offsets.topic.num.partitions", String.valueOf(1));
                  configs.put("offsets.topic.replication.factor", String.valueOf(1));
                  configs.put("log.dirs", String.join(",", tempFolders.get(entry.getKey())));

                  // add custom configs
                  configs.putAll(override);

                  return server(
                      configs, tempFolders.get(entry.getKey()), clusterId, entry.getKey());
                })
            .collect(Collectors.toList());
    var brokers =
        brokersFutures.stream()
            .map(CompletableFuture::join)
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    AtomicReference<String> connectionProps = new AtomicReference<>();
    connectionProps.set(
        availablePorts.entrySet().stream()
            .map(entry -> "localhost:" + entry.getValue())
            .collect(Collectors.joining(",")));
    return new BrokerCluster() {
      @Override
      public void close(int brokerID) {
        var broker = brokers.remove(brokerID);
        if (broker != null) {
          broker.shutdown();
          broker.awaitShutdown();
          var folders = tempFolders.remove(brokerID);
          if (folders != null) folders.forEach(f -> Utils.delete(Path.of(f)));
          connectionProps.set(
              availablePorts.entrySet().stream()
                  .filter(entry -> !entry.getKey().equals(brokerID))
                  .map(entry -> "localhost:" + entry.getValue())
                  .collect(Collectors.joining(",")));
        }
      }

      @Override
      public void close() {
        Set.copyOf(brokers.keySet()).forEach(this::close);
        controller.shutdown();
        controller.awaitShutdown();
        Utils.delete(Path.of(controllerFolder));
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
