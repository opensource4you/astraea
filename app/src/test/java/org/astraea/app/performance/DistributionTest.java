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

import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class DistributionTest {

  @Test
  void testFixed() {
    var distribution = DistributionType.FIXED.create(new Random().nextInt());
    Assertions.assertEquals(
        1,
        IntStream.range(0, 10)
            .mapToObj(ignored -> distribution.get())
            .collect(Collectors.toSet())
            .size());
  }

  @Test
  void testUniform() {
    var distribution = DistributionType.UNIFORM.create(5);
    Assertions.assertTrue(distribution.get() < 5);
    Assertions.assertTrue(distribution.get() >= 0);
  }

  @Test
  void testLatest() throws InterruptedException {
    var distribution = DistributionType.LATEST.create(Integer.MAX_VALUE);
    Assertions.assertEquals(distribution.get(), distribution.get());

    long first = distribution.get();
    Thread.sleep(2000);

    Assertions.assertNotEquals(first, distribution.get());
  }

  @Test
  void testZipfian() {
    var distribution = DistributionType.ZIPFIAN.create(5);
    Assertions.assertTrue(distribution.get() < 5);
    Assertions.assertTrue(distribution.get() >= 0);
  }
}
