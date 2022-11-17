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
package org.astraea.connector;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.astraea.common.cost.Configuration;
import org.astraea.common.http.HttpExecutor;
import org.astraea.common.json.TypeRef;
import org.astraea.common.producer.Record;
import org.astraea.it.RequireSingleWorkerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SourceConnectorTest extends RequireSingleWorkerCluster {

  @Test
  void testRun() {
    var exec = HttpExecutor.builder().build();
    var r =
        exec.get(workerUrl().toString() + "connector-plugins", TypeRef.array(Map.class))
            .toCompletableFuture()
            .join();
    Assertions.assertNotEquals(0, r.body().size());
    // TODO: use connector client to rewrite test after
    // https://github.com/skiptests/astraea/pull/1012 gets merged
    Assertions.assertTrue(
        r.body().stream()
            .map(m -> (Map<String, String>) m)
            .anyMatch(m -> m.get("class").equals(MyConnector.class.getName())));
  }

  public static class MyConnector extends SourceConnector {

    @Override
    protected Class<? extends SourceTask> task() {
      return MyTask.class;
    }

    @Override
    protected List<Configuration> takeConfiguration(int maxTasks) {
      return List.of(Configuration.of(Map.of()));
    }

    @Override
    protected List<Definition> definitions() {
      return List.of();
    }
  }

  public static class MyTask extends SourceTask {

    @Override
    protected Collection<Record<byte[], byte[]>> take() throws InterruptedException {
      return List.of();
    }
  }
}
