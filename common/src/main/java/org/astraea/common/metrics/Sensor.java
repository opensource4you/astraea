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
package org.astraea.common.metrics;

import java.util.HashMap;
import java.util.Map;
import org.astraea.common.metrics.stats.Stat;

public class Sensor<V> {
  private final Map<String, Stat<V>> stats = new HashMap<>();

  public void record(V value) {
    stats.values().forEach(stat -> stat.record(value));
  }

  public void add(String metricName, Stat<V> stat) {
    stats.put(metricName, stat);
  }

  public V measure(String metricName) {
    return stats.get(metricName).measure();
  }

  public Map<String, Stat<V>> metrics() {
    return stats;
  }
}
