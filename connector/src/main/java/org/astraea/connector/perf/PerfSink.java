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

import java.time.Duration;
import java.util.List;
import java.util.stream.IntStream;
import org.astraea.common.Configuration;
import org.astraea.common.Utils;
import org.astraea.common.consumer.Record;
import org.astraea.connector.Definition;
import org.astraea.connector.SinkConnector;
import org.astraea.connector.SinkTask;

public class PerfSink extends SinkConnector {

  static Duration FREQUENCY_DEFAULT = Duration.ofMillis(300);

  static Definition FREQUENCY_DEF =
      Definition.builder()
          .name("frequency")
          .type(Definition.Type.STRING)
          .defaultValue(FREQUENCY_DEFAULT.toMillis() + "ms")
          .validator((name, value) -> Utils.toDuration(value.toString()))
          .build();

  private Configuration config;

  @Override
  protected void init(Configuration configuration) {
    this.config = configuration;
  }

  @Override
  protected Class<? extends SinkTask> task() {
    return Task.class;
  }

  @Override
  protected List<Configuration> takeConfiguration(int maxTasks) {
    return IntStream.range(0, maxTasks).mapToObj(i -> config).toList();
  }

  @Override
  protected List<Definition> definitions() {
    return List.of(FREQUENCY_DEF);
  }

  public static class Task extends SinkTask {

    private Duration frequency = FREQUENCY_DEFAULT;

    private volatile long lastPut = System.currentTimeMillis();

    @Override
    protected void init(Configuration configuration) {
      frequency =
          configuration.string(FREQUENCY_DEF.name()).map(Utils::toDuration).orElse(frequency);
    }

    @Override
    protected void put(List<Record<byte[], byte[]>> records) {
      var now = System.currentTimeMillis();
      var diff = frequency.toMillis() - (now - lastPut);
      if (diff > 0) Utils.sleep(Duration.ofMillis(diff));
      lastPut = now;
    }
  }
}
