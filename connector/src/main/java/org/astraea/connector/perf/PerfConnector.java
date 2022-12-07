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
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.astraea.common.Configuration;
import org.astraea.common.Utils;
import org.astraea.connector.Definition;
import org.astraea.connector.SourceConnector;
import org.astraea.connector.SourceTask;

public class PerfConnector extends SourceConnector {
  static Definition FREQUENCY_DEF =
      Definition.builder()
          .name("frequency.in.seconds")
          .type(Definition.Type.STRING)
          .defaultValue("1s")
          .validator((name, value) -> Utils.toDuration(value.toString()))
          .build();
  static Definition KEY_LENGTH_DEF =
      Definition.builder().name("key.length").type(Definition.Type.INT).defaultValue(10).build();
  static Definition VALUE_LENGTH_DEF =
      Definition.builder().name("value.length").type(Definition.Type.INT).defaultValue(10).build();

  private Configuration config;

  @Override
  protected void init(Configuration configuration) {
    this.config = configuration;
  }

  @Override
  protected Class<? extends SourceTask> task() {
    return PerfTask.class;
  }

  @Override
  protected List<Configuration> takeConfiguration(int maxTasks) {
    return IntStream.range(0, maxTasks).mapToObj(i -> config).collect(Collectors.toList());
  }

  @Override
  protected List<Definition> definitions() {
    return List.of(FREQUENCY_DEF, KEY_LENGTH_DEF, VALUE_LENGTH_DEF);
  }
}
