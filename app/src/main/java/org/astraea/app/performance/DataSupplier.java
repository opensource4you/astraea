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

import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import org.astraea.common.DataRate;

@FunctionalInterface
interface DataSupplier extends Supplier<DataSupplier.Data> {

  static Data data(byte[] key, byte[] value) {
    return new Data() {
      @Override
      public boolean done() {
        return false;
      }

      @Override
      public boolean throttled() {
        return false;
      }

      @Override
      public byte[] key() {
        return key;
      }

      @Override
      public byte[] value() {
        return value;
      }
    };
  }

  Data NO_MORE_DATA =
      new Data() {
        @Override
        public boolean done() {
          return true;
        }

        @Override
        public boolean throttled() {
          return false;
        }

        @Override
        public byte[] key() {
          throw new IllegalStateException("there is no data");
        }

        @Override
        public byte[] value() {
          throw new IllegalStateException("there is no data");
        }
      };

  Data THROTTLED_DATA =
      new Data() {
        @Override
        public boolean done() {
          return false;
        }

        @Override
        public boolean throttled() {
          return true;
        }

        @Override
        public byte[] key() {
          throw new IllegalStateException("it is throttled");
        }

        @Override
        public byte[] value() {
          throw new IllegalStateException("it is throttled");
        }
      };

  interface Data {

    /**
     * @return true if there is no data.
     */
    boolean done();

    /**
     * @return true if there are some data, but it is throttled now.
     */
    boolean throttled();

    /**
     * @return true if there is accessible data
     */
    default boolean hasData() {
      return !done() && !throttled();
    }

    /**
     * @return key or throw exception if there is no data, or it is throttled now
     */
    byte[] key();

    /**
     * @return value or throw exception if there is no data, or it is throttled now
     */
    byte[] value();
  }

  /**
   * Generate Data according to the given arguments. The returned supplier map the 64-bit number
   * supplied by key(/value) distribution to a byte array. That is, if we want the DataSupplier to
   * produce the same content, the key(/value) distribution should always produce the same number.
   * For example,
   *
   * <pre>{@code
   * DataSupplier.of(exeTime,
   *                 ()->1,
   *                 keySizeDistribution,
   *                 ()->1,
   *                 valueSizeDistribution,
   *                 throughput)
   * }</pre>
   *
   * It is not recommend to supply too many unique number. This DataSupplier store every unique
   * number and its content in a map structure.
   *
   * @param exeTime the time for stop supplying data
   * @param keyDistribution supply abstract keys which is represented by a 64-bit integer
   * @param keySizeDistribution supply the size of newly created key
   * @param valueDistribution supply abstract value which is represented by a 64-bit integer
   * @param valueSizeDistribution supply the size of newly created value
   * @param throughput the limit on data produced
   * @return supply data with given distribution. It will map the 64-bit number supplied by
   *     key(/value) distribution to a byte array.
   */
  static DataSupplier of(
      ExeTime exeTime,
      Supplier<Long> keyDistribution,
      Supplier<Long> keySizeDistribution,
      Supplier<Long> valueDistribution,
      Supplier<Long> valueSizeDistribution,
      DataRate throughput) {
    return new DataSupplier() {
      private final Throttler throttler = new Throttler(throughput);
      private final long start = System.currentTimeMillis();
      private final Random rand = new Random();
      private final AtomicLong dataCount = new AtomicLong(0);
      private final Map<Long, byte[]> recordKeyTable = new ConcurrentHashMap<>();
      private final Map<Long, byte[]> recordValueTable = new ConcurrentHashMap<>();

      byte[] value() {
        return getOrNew(
            recordValueTable, valueDistribution, valueSizeDistribution.get().intValue(), rand);
      }

      public byte[] key() {
        return getOrNew(
            recordKeyTable, keyDistribution, keySizeDistribution.get().intValue(), rand);
      }

      @Override
      public Data get() {
        if (exeTime.percentage(dataCount.getAndIncrement(), System.currentTimeMillis() - start)
            >= 100D) return NO_MORE_DATA;
        var key = key();
        var value = value();
        if (throttler.throttled(
            (value != null ? value.length : 0) + (key != null ? key.length : 0)))
          return THROTTLED_DATA;
        return data(key, value);
      }
    };
  }

  // Find the key from the table, if the record has been produced before. Randomly generate a
  // byte array if
  // the record has not been produced.
  private static byte[] getOrNew(
      Map<Long, byte[]> table, Supplier<Long> distribution, int size, Random rand) {
    return table.computeIfAbsent(
        distribution.get(),
        ignore -> {
          if (size == 0) return null;
          var value = new byte[size];
          rand.nextBytes(value);
          return value;
        });
  }

  class Throttler {
    private final long start = System.currentTimeMillis();
    private final long throughput;
    private final AtomicLong totalBytes = new AtomicLong();

    Throttler(DataRate max) {
      throughput = Double.valueOf(max.byteRate()).longValue();
    }

    /**
     * @param payloadLength of new data
     * @return true if the data need to be throttled. Otherwise, false
     */
    boolean throttled(long payloadLength) {
      var duration = durationInSeconds();
      if (duration <= 0) return false;
      var current = totalBytes.addAndGet(payloadLength);
      // too much -> slow down
      if ((current / duration) > throughput) {
        totalBytes.addAndGet(-payloadLength);
        return true;
      }
      return false;
    }

    // visible for testing
    long durationInSeconds() {
      return (System.currentTimeMillis() - start) / 1000;
    }
  }
}
