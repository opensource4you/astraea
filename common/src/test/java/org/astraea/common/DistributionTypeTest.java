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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class DistributionTypeTest {

  @Test
  void testZero() {
    Arrays.stream(DistributionType.values())
        .forEach(
            d -> {
              Assertions.assertEquals(0, d.create(0, Configuration.EMPTY).get());
              Assertions.assertEquals(0, d.create(-100, Configuration.EMPTY).get());
            });
  }

  @Test
  void testFixed() {
    var distribution = DistributionType.FIXED.create(new Random().nextInt(), Configuration.EMPTY);
    Assertions.assertEquals(
        1,
        IntStream.range(0, 10)
            .mapToObj(ignored -> distribution.get())
            .collect(Collectors.toSet())
            .size());
  }

  @Test
  void testUniform() {
    var distribution = DistributionType.UNIFORM.create(5, Configuration.EMPTY);
    Assertions.assertTrue(distribution.get() < 5);
    Assertions.assertTrue(distribution.get() >= 0);
  }

  @Test
  void testLatest() throws InterruptedException {
    var distribution = DistributionType.LATEST.create(Integer.MAX_VALUE, Configuration.EMPTY);
    Assertions.assertEquals(distribution.get(), distribution.get());

    long first = distribution.get();
    Thread.sleep(2000);

    Assertions.assertNotEquals(first, distribution.get());
  }

  @Test
  void testZipfian() {
    var distribution = DistributionType.ZIPFIAN.create(5, Configuration.EMPTY);
    Assertions.assertTrue(distribution.get() < 5);
    Assertions.assertTrue(distribution.get() >= 0);

    // The last cumulative probability should not less than 1.0
    for (int i = 1000; i < 2000; ++i) {
      distribution = DistributionType.ZIPFIAN.create(i, Configuration.EMPTY);
      for (int j = 0; j < 1000; ++j) {
        Assertions.assertTrue(distribution.get() < i);
        Assertions.assertTrue(distribution.get() >= 0);
      }
    }
  }

  @Test
  void testZipfianConfig() {
    // fixed seed
    var zip100 =
        DistributionType.ZIPFIAN.create(
            10000, Configuration.of(Map.of(DistributionType.ZIPFIAN_SEED, "100")));
    Assertions.assertEquals(
        List.of(11, 18, 0, 1126, 12),
        IntStream.range(0, 5)
            .map(i -> zip100.get().intValue())
            .boxed()
            .collect(Collectors.toUnmodifiableList()),
        "Random sequence fixed by specific seed");

    // random fixed seed
    var seed = ThreadLocalRandom.current().nextInt();
    var zipA =
        DistributionType.ZIPFIAN.create(
            10000, Configuration.of(Map.of(DistributionType.ZIPFIAN_SEED, Integer.toString(seed))));
    var zipB =
        DistributionType.ZIPFIAN.create(
            10000, Configuration.of(Map.of(DistributionType.ZIPFIAN_SEED, Integer.toString(seed))));
    var sequenceA = IntStream.range(0, 1000).map(i -> zipA.get().intValue()).toArray();
    var sequenceB = IntStream.range(0, 1000).map(i -> zipB.get().intValue()).toArray();
    Assertions.assertArrayEquals(sequenceA, sequenceB);

    // high exponent come with high skewness
    var zip1 =
        DistributionType.ZIPFIAN.create(
            100, Configuration.of(Map.of(DistributionType.ZIPFIAN_EXPONENT, Double.toString(1.0))));
    var zip2 =
        DistributionType.ZIPFIAN.create(
            100, Configuration.of(Map.of(DistributionType.ZIPFIAN_EXPONENT, Double.toString(2.0))));
    var counting1 =
        IntStream.range(0, 10000)
            .map(x -> zip1.get().intValue())
            .boxed()
            .collect(Collectors.groupingBy(x -> x, Collectors.counting()));
    var counting2 =
        IntStream.range(0, 10000)
            .map(x -> zip2.get().intValue())
            .boxed()
            .collect(Collectors.groupingBy(x -> x, Collectors.counting()));
    Assertions.assertTrue(
        counting2.get(0) > counting1.get(0),
        "zipf with exp 2.0 should be much skewer than the one with exp 1.0");
  }
}
