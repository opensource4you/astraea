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
package org.astraea.common.connector;

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.function.Predicate;
import org.astraea.common.Utils;
import org.astraea.common.http.HttpRequestException;

/**
 * The kafka connect client
 *
 * @see <a
 *     href="https://docs.confluent.io/platform/current/connect/references/restapi.html">Connector
 *     Document</a>
 */
public interface ConnectorClient {

  String CONNECTOR_CLASS_KEY = "connector.class";
  String TASK_MAX_KEY = "tasks.max";
  String TOPICS_KEY = "topics";

  static Builder builder() {
    return new Builder();
  }

  CompletionStage<WorkerInfo> info();

  CompletionStage<Set<String>> connectorNames();

  CompletionStage<ConnectorInfo> connectorInfo(String name);

  CompletionStage<ConnectorStatus> connectorStatus(String name);

  CompletionStage<ConnectorInfo> createConnector(String name, Map<String, String> config);

  CompletionStage<ConnectorInfo> updateConnector(String name, Map<String, String> config);

  CompletionStage<Void> deleteConnector(String name);

  CompletionStage<Validation> validate(String name, Map<String, String> configs);

  CompletionStage<Set<PluginInfo>> plugins();

  default CompletionStage<Boolean> waitConnectorInfo(
      String connectName, Predicate<ConnectorStatus> predicate, Duration timeout) {
    return Utils.loop(
        () ->
            connectorStatus(connectName)
                .thenApply(predicate::test)
                .exceptionally(
                    e -> {
                      if (e instanceof HttpRequestException
                          || e.getCause() instanceof HttpRequestException) return false;

                      if (e instanceof RuntimeException) throw (RuntimeException) e;
                      throw new RuntimeException(e);
                    }),
        timeout.toMillis(),
        2);
  }
}
