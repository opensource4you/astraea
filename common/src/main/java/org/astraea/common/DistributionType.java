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
      cumulativeDensityTable.add(1D / H_N);
      IntStream.range(1, n)
          .forEach(
              i ->
                  cumulativeDensityTable.add(
                      cumulativeDensityTable.get(i - 1) + 1D / (i + 1) / H_N));
      return () -> {
        final double randNum = rand.nextDouble();
        for (int i = 0; i < cumulativeDensityTable.size(); ++i) {
          if (randNum < cumulativeDensityTable.get(i)) return (long) i;
        }
        return (long) cumulativeDensityTable.size() - 1L;
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
