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
package org.astraea.common;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;
import java.util.function.Supplier;
import java.util.stream.IntStream;

/**
 * Random distribution generator. Example: {@code Supplier<long> uniformDistribution =
 * Distribution.UNIFORM.create(100); while (true){ // the value will in range [0, 100)
 * uniformDistribution.get(); } }
 */
public enum DistributionType implements EnumInfo {
  FIXED {
    @Override
    public Supplier<Long> supplier(int n) {
      return () -> (long) n;
    }
  },

  UNIFORM {
    @Override
    public Supplier<Long> supplier(int n) {
      var rand = new Random();
      return () -> (long) rand.nextInt(n);
    }
  },

  /** A distribution for providing different random value every 2 seconds */
  LATEST {
    @Override
    public Supplier<Long> supplier(int n) {
      var rand = new Random();
      return () -> {
        var time = System.currentTimeMillis();
        rand.setSeed(time - time % 2000);
        return (long) rand.nextInt(n);
      };
    }
  },

  /**
   * Building a zipfian distribution with PDF: 1/k/H_N, where H_N is the Nth harmonic number (= 1/1
   * + 1/2 + ... + 1/N); k is the key id
   */
  ZIPFIAN {
    @Override
    public Supplier<Long> supplier(int n) {
      var cumulativeDensityTable = new ArrayList<Double>();
      var rand = new Random();
      var H_N = IntStream.range(1, n + 1).mapToDouble(k -> 1D / k).sum();
      var sum = 0D;
      // In theory, the last entry of cumulative density table is 1.0. But due to the precision of
      // floating point addition, the resulting last entry may not be 1.0. Here, we manually fix the
      // last entry to 1.0. Then, the binary search afterward will not return the index out of
      // expected range.
      for (var i = 1L; i < n; ++i) {
        sum += 1D / i / H_N;
        cumulativeDensityTable.add(sum);
      }
      cumulativeDensityTable.add(1.0);
      return () -> {
        var randNum = rand.nextDouble();
        var ret = (long) Collections.binarySearch(cumulativeDensityTable, randNum);
        return (ret >= 0) ? ret : (-ret - 1);
      };
    }
  };

  public static DistributionType ofAlias(String alias) {
    return EnumInfo.ignoreCaseEnum(DistributionType.class, alias);
  }

  @Override
  public String alias() {
    return name();
  }

  @Override
  public String toString() {
    return alias();
  }

  public final Supplier<Long> create(int n) {
    if (n <= 0) return () -> 0L;
    return supplier(n);
  }

  protected abstract Supplier<Long> supplier(int n);
}
