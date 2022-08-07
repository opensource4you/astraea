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
package org.astraea.app.performance;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.astraea.app.admin.TopicPartition;

/** Used to record statistics. This is thread safe. */
public class Report {
  /**
   * find the max offset from reports
   *
   * @param reports to search offsets
   * @return topic partition and its max offset
   */
  public static Map<TopicPartition, Long> maxOffsets(List<Report> reports) {
    return reports.stream()
        .flatMap(r -> r.maxOffsets().entrySet().stream())
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, Math::max, HashMap::new));
  }

  private double avgLatency = 0;
  private long records = 0;
  private long max = 0;
  private long min = Long.MAX_VALUE;
  private long totalBytes = 0;
  private long currentBytes = 0;
  private final Map<TopicPartition, Long> currentOffsets = new HashMap<>();

  /** Simultaneously add latency and bytes. */
  public synchronized void record(
      String topic, int partition, long offset, long latency, int bytes) {
    ++records;
    min = Math.min(min, latency);
    max = Math.max(max, latency);
    avgLatency += (((double) latency) - avgLatency) / (double) records;
    currentBytes += bytes;
    totalBytes += bytes;
    var tp = TopicPartition.of(topic, partition);
    currentOffsets.put(tp, Math.max(offset, offset(tp)));
  }

  /** @return Get the number of records. */
  public synchronized long records() {
    return records;
  }
  /** @return Get the maximum of latency put. */
  public synchronized long max() {
    return max;
  }
  /** @return Get the minimum of latency put. */
  public synchronized long min() {
    return min;
  }
  /** @return Get the average latency. */
  public synchronized double avgLatency() {
    return avgLatency;
  }

  /** @return total send/received bytes */
  public synchronized long totalBytes() {
    return totalBytes;
  }

  public synchronized long clearAndGetCurrentBytes() {
    var ans = currentBytes;
    currentBytes = 0;
    return ans;
  }

  public synchronized long offset(TopicPartition tp) {
    return currentOffsets.getOrDefault(tp, 0L);
  }

  public synchronized Map<TopicPartition, Long> maxOffsets() {
    return Map.copyOf(currentOffsets);
  }
}
