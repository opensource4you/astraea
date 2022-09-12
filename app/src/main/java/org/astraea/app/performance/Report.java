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

import java.util.function.Supplier;
import org.astraea.common.metrics.MBeanClient;
import org.astraea.common.metrics.client.consumer.ConsumerMetrics;
import org.astraea.common.metrics.client.consumer.HasConsumerFetchMetrics;

public interface Report {

  static long recordsConsumedTotal() {
    var client = MBeanClient.local();
    return (long)
        ConsumerMetrics.fetches(client).stream()
            .mapToDouble(HasConsumerFetchMetrics::recordsConsumedTotal)
            .sum();
  }

  /** @return Get the number of records. */
  long records();
  /** @return Get the maximum of latency put. */
  long max();
  /** @return Get the minimum of latency put. */
  long min();
  /** @return Get the average latency. */
  double avgLatency();

  /** @return total send/received bytes */
  long totalBytes();

  boolean isClosed();

  String clientId();

  void record(long latency, int bytes);

  static Report of(String clientId, Supplier<Boolean> isClosed) {
    return new Report() {

      private double avgLatency = 0;
      private long records = 0;
      private long max = 0;
      private long min = Long.MAX_VALUE;
      private long totalBytes = 0;

      @Override
      public synchronized void record(long latency, int bytes) {
        ++records;
        min = Math.min(min, latency);
        max = Math.max(max, latency);
        avgLatency += (((double) latency) - avgLatency) / (double) records;
        totalBytes += bytes;
      }

      /** @return Get the number of records. */
      @Override
      public synchronized long records() {
        return records;
      }
      /** @return Get the maximum of latency put. */
      @Override
      public synchronized long max() {
        return max;
      }
      /** @return Get the minimum of latency put. */
      @Override
      public synchronized long min() {
        return min;
      }

      /** @return Get the average latency. */
      @Override
      public synchronized double avgLatency() {
        return avgLatency;
      }

      /** @return total send/received bytes */
      @Override
      public synchronized long totalBytes() {
        return totalBytes;
      }

      @Override
      public boolean isClosed() {
        return isClosed.get();
      }

      @Override
      public String clientId() {
        return clientId;
      }
    };
  }
}
