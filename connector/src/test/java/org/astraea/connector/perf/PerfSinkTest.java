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
package org.astraea.connector.perf;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.astraea.common.Configuration;
import org.astraea.common.Utils;
import org.astraea.common.connector.ConnectorClient;
import org.astraea.common.consumer.Record;
import org.astraea.it.RequireSingleWorkerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class PerfSinkTest extends RequireSingleWorkerCluster {

  @Test
  void testDefaultConfig() {
    var client = ConnectorClient.builder().url(workerUrl()).build();
    var validation =
        client
            .validate(
                PerfSink.class.getSimpleName(),
                Map.of(
                    ConnectorClient.NAME_KEY,
                    Utils.randomString(),
                    ConnectorClient.CONNECTOR_CLASS_KEY,
                    PerfSink.class.getName(),
                    ConnectorClient.TASK_MAX_KEY,
                    "1",
                    ConnectorClient.TOPICS_KEY,
                    "abc"))
            .toCompletableFuture()
            .join();
    Assertions.assertEquals(
        0,
        validation.errorCount(),
        validation.configs().stream()
            .flatMap(c -> c.value().errors().stream())
            .collect(Collectors.joining(",")));
  }

  @Test
  void testFrequency() {
    var client = ConnectorClient.builder().url(workerUrl()).build();
    var validation =
        client
            .validate(
                PerfSink.class.getSimpleName(),
                Map.of(
                    ConnectorClient.NAME_KEY,
                    Utils.randomString(),
                    ConnectorClient.CONNECTOR_CLASS_KEY,
                    PerfSink.class.getName(),
                    ConnectorClient.TASK_MAX_KEY,
                    "1",
                    ConnectorClient.TOPICS_KEY,
                    "abc",
                    PerfSink.FREQUENCY_DEF.name(),
                    "a"))
            .toCompletableFuture()
            .join();
    Assertions.assertEquals(1, validation.errorCount());
    Assertions.assertNotEquals(
        0,
        validation.configs().stream()
            .filter(c -> c.definition().name().equals(PerfSink.FREQUENCY_DEF.name()))
            .findFirst()
            .get()
            .value()
            .errors()
            .size());
  }

  @Test
  void testTask() {
    var task = new PerfSinkTask();
    task.init(Configuration.of(Map.of(PerfSink.FREQUENCY_DEF.name(), "1s")));

    var now = System.currentTimeMillis();
    task.put(List.<Record<byte[], byte[]>>of());

    // the frequency is 1s so the elapsed time of executing put should be greater than 500ms
    Assertions.assertTrue(System.currentTimeMillis() - now > 500);
  }
}
