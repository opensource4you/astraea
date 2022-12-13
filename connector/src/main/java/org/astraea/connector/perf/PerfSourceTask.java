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

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.astraea.common.Configuration;
import org.astraea.common.Utils;
import org.astraea.common.producer.Record;
import org.astraea.connector.SourceConnector;
import org.astraea.connector.SourceTask;

public class PerfSourceTask extends SourceTask {

  private Set<String> topics = Set.of();
  private int keyLength = (int) PerfSource.KEY_LENGTH_DEF.defaultValue();
  private int valueLength = (int) PerfSource.KEY_LENGTH_DEF.defaultValue();
  private Duration frequency = Utils.toDuration(PerfSource.FREQUENCY_DEF.defaultValue().toString());
  private long last = System.currentTimeMillis();

  @Override
  protected void init(Configuration configuration) {
    this.topics = Set.copyOf(configuration.list(SourceConnector.TOPICS_KEY, ","));
    this.keyLength = configuration.integer(PerfSource.KEY_LENGTH_DEF.name()).orElse(keyLength);
    this.valueLength =
        configuration.integer(PerfSource.VALUE_LENGTH_DEF.name()).orElse(valueLength);
    this.frequency =
        configuration
            .string(PerfSource.FREQUENCY_DEF.name())
            .map(Utils::toDuration)
            .orElse(frequency);
  }

  @Override
  protected Collection<Record<byte[], byte[]>> take() {
    if (System.currentTimeMillis() - last < frequency.toMillis()) return List.of();
    try {
      return topics.stream()
          .map(
              t ->
                  Record.builder()
                      .topic(t)
                      .key(Utils.randomString(keyLength).getBytes(StandardCharsets.UTF_8))
                      .value(Utils.randomString(valueLength).getBytes(StandardCharsets.UTF_8))
                      .build())
          .collect(Collectors.toList());
    } finally {
      last = System.currentTimeMillis();
    }
  }
}
