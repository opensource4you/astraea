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
import org.astraea.common.Configuration;
import org.astraea.common.Utils;
import org.astraea.common.consumer.Record;
import org.astraea.connector.SinkTask;

public class PerfSinkTask extends SinkTask {

  private Duration frequency = Utils.toDuration(PerfSink.FREQUENCY_DEF.defaultValue().toString());

  private volatile long lastPut = System.currentTimeMillis();

  @Override
  protected void init(Configuration configuration) {
    frequency =
        configuration
            .string(PerfSource.FREQUENCY_DEF.name())
            .map(Utils::toDuration)
            .orElse(frequency);
  }

  @Override
  protected void put(List<Record<byte[], byte[]>> records) {
    var now = System.currentTimeMillis();
    var diff = frequency.toMillis() - (now - lastPut);
    if (diff > 0) Utils.sleep(Duration.ofMillis(diff));
    lastPut = now;
  }
}
