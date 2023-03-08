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

import java.util.Random;
import java.util.function.Supplier;
import org.apache.commons.math3.distribution.ZipfDistribution;
import org.apache.commons.math3.random.Well19937c;

/**
 * Random distribution generator. Example: {@code Supplier<long> uniformDistribution =
 * Distribution.UNIFORM.create(100); while (true){ // the value will in range [0, 100)
 * uniformDistribution.get(); } }
 */
public enum DistributionType implements EnumInfo {
  FIXED {
    @Override
    public Supplier<Long> supplier(int n, Configuration configuration) {
      return () -> (long) n;
    }
  },

  UNIFORM {
    @Override
    public Supplier<Long> supplier(int n, Configuration configuration) {
      var rand = new Random();
      return () -> (long) rand.nextInt(n);
    }
  },

  /** A distribution for providing different random value every 2 seconds */
  LATEST {
    @Override
    public Supplier<Long> supplier(int n, Configuration configuration) {
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
    public Supplier<Long> supplier(int n, Configuration configuration) {
      var exponent = configuration.string(ZIPFIAN_EXPONENT).map(Double::parseDouble).orElse(1.0);
      var rng = configuration.integer(ZIPFIAN_SEED).map(Well19937c::new).orElse(new Well19937c());
      var distribution = new ZipfDistribution(rng, n, exponent);

      // the zipf in common math3 has function range [1, n]. But this previous implementation of our
      // zipf expects [0, n), so we explicitly minus it by one to comply with the implementation
      // detail.
      var correction = 1;
      return () -> (long) (distribution.sample() - correction);
    }
  };

  public static final String ZIPFIAN_SEED = "seed";
  public static final String ZIPFIAN_EXPONENT = "exponent";

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

  public final Supplier<Long> create(int n, Configuration configuration) {
    if (n <= 0) return () -> 0L;
    return supplier(n, configuration);
  }

  protected abstract Supplier<Long> supplier(int n, Configuration config);
}
