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

import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.connect.cli.ConnectDistributed;
import org.apache.kafka.connect.runtime.Connect;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.distributed.DistributedConfig;

public interface WorkerCluster extends AutoCloseable {

  static WorkerCluster of(
      BrokerCluster brokerCluster, int numberOfWorkers, Map<String, String> override) {
    List<Connect> connects =
        IntStream.range(0, numberOfWorkers)
            .mapToObj(
                index -> {
                  var realPort = Utils.availablePort();
                  var configs = new HashMap<String, String>();
                  // reduce the number from partitions and replicas to speedup the mini cluster
                  // for setting storage. the partition from setting topic is always 1 so we
                  // needn't to set it to 1 here.
                  configs.put(DistributedConfig.CONFIG_TOPIC_CONFIG, "connect-config");
                  configs.put(
                      DistributedConfig.CONFIG_STORAGE_REPLICATION_FACTOR_CONFIG,
                      String.valueOf(brokerCluster.dataFolders().size()));
                  // for offset storage
                  configs.put(DistributedConfig.OFFSET_STORAGE_TOPIC_CONFIG, "connect-offsets");
                  configs.put(DistributedConfig.OFFSET_STORAGE_PARTITIONS_CONFIG, "1");
                  configs.put(
                      DistributedConfig.OFFSET_STORAGE_REPLICATION_FACTOR_CONFIG,
                      String.valueOf(brokerCluster.dataFolders().size()));
                  // for status storage
                  configs.put(DistributedConfig.STATUS_STORAGE_TOPIC_CONFIG, "connect-status");
                  configs.put(DistributedConfig.STATUS_STORAGE_PARTITIONS_CONFIG, "1");
                  configs.put(
                      DistributedConfig.STATUS_STORAGE_REPLICATION_FACTOR_CONFIG,
                      String.valueOf(brokerCluster.dataFolders().size()));
                  // set the brokers info
                  configs.put(
                      WorkerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerCluster.bootstrapServers());
                  configs.put(DistributedConfig.GROUP_ID_CONFIG, "connect");
                  // set the normal converter
                  configs.put(
                      ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG,
                      "org.apache.kafka.connect.json.JsonConverter");
                  configs.put(
                      ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG,
                      "org.apache.kafka.connect.json.JsonConverter");
                  configs.put(
                      WorkerConfig.LISTENERS_CONFIG,
                      // the worker hostname is a part of information used by restful apis.
                      // the 0.0.0.0 make all connector say that they are executed by 0.0.0.0
                      // and it does make sense in production. With a view to testing the
                      // related codes in other modules, we have to define the "really" hostname
                      // in starting worker cluster.
                      "http://" + Utils.hostname() + ":" + realPort);
                  configs.put(WorkerConfig.OFFSET_COMMIT_INTERVAL_MS_CONFIG, String.valueOf(500));
                  // enable us to override the connector configs
                  configs.put(WorkerConfig.CONNECTOR_CLIENT_POLICY_CLASS_CONFIG, "All");

                  // add custom configs
                  configs.putAll(override);
                  return new ConnectDistributed().startConnect(configs);
                })
            .collect(Collectors.toUnmodifiableList());

    return new WorkerCluster() {
      @Override
      public Set<URL> workerUrls() {
        return connects.stream()
            .map(c -> c.rest().advertisedUrl())
            .map(
                uri -> {
                  try {
                    return uri.toURL();
                  } catch (MalformedURLException e) {
                    throw new RuntimeException(e);
                  }
                })
            .collect(Collectors.toUnmodifiableSet());
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

  /**
   * @return worker information
   */
  Set<URL> workerUrls();

  @Override
  void close();
}
