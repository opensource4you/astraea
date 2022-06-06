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
package org.astraea.app.cost;

import java.util.Comparator;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class WeightProviderTest {

  @ParameterizedTest
  @MethodSource("normalizers")
  void testEntropy(Normalizer normalizer) {
    var length = 8;
    var numberOfMetrics = 2;
    var numberOfObjects = 2;
    var weightProvider = new WeightProvider.EntropyWeightProvider(normalizer);
    var raw =
        IntStream.range(0, numberOfMetrics)
            .boxed()
            .collect(
                Collectors.toMap(
                    String::valueOf,
                    ignored ->
                        IntStream.range(0, numberOfObjects)
                            .mapToObj(i -> Math.random())
                            .collect(Collectors.toUnmodifiableList())));
    // The smaller entropy means larger weight, so we sort the entropies/weights by different order
    // to check the metrics name later
    var entropies =
        weightProvider.entropies(raw).entrySet().stream()
            .sorted(Map.Entry.comparingByValue())
            .collect(Collectors.toUnmodifiableList());

    entropies.forEach(e -> Assertions.assertTrue(0 <= e.getValue() && e.getValue() <= 1));

    var weights =
        weightProvider.weight(raw).entrySet().stream()
            .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
            .collect(Collectors.toUnmodifiableList());
    Assertions.assertEquals(entropies.size(), weights.size());
    IntStream.range(0, entropies.size())
        .forEach(i -> Assertions.assertEquals(entropies.get(i).getKey(), weights.get(i).getKey()));
  }

  private static Stream<Arguments> normalizers() {
    return Normalizer.all().stream().map(Arguments::of);
  }
}
