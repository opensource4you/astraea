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
package org.astraea.common.cost;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class WeightProviderTest {
  @Test
  void testPositiveEntropy() {
    Normalizer normalizer = Normalizer.minMax(true);
    var weightProvider = new WeightProvider.EntropyWeightProvider(normalizer);
    var confusion =
        Map.of(
            "0",
            IntStream.range(0, 100)
                .mapToObj(i -> Math.random() * i)
                .collect(Collectors.toUnmodifiableList()),
            "1",
            IntStream.range(0, 100).mapToObj(i -> 1.0).collect(Collectors.toUnmodifiableList()));

    var entropy = weightProvider.weight(confusion);
    // "0" range of 0 to 100 numbers * Math.random().
    // "1" range of 0 to 1 numbers.The number is accurate to one decimal places.
    // "0" is more confusing than "1".
    Assertions.assertTrue(entropy.get("0") > entropy.get("1"));

    var metrics1 = Math.random();
    var metrics2 = Math.random();
    var metrics3 = Math.random();
    var metrics =
        Map.of(
            0,
            List.of(metrics1, metrics2, metrics3),
            1,
            List.of(metrics1 * 100, metrics2 * 100, metrics3 * 100));
    var sameEntropy = weightProvider.weight(metrics);
    Assertions.assertEquals(
        Math.round(sameEntropy.get(0) * 100) / 100, Math.round(sameEntropy.get(1) * 100) / 100);

    var AABAndABB =
        Map.of(
            0,
            List.of(metrics1 * 10, metrics2, metrics2),
            1,
            List.of(metrics1 * 10, metrics1 * 10, metrics2));
    var ABEntropy = weightProvider.weight(AABAndABB);
    Assertions.assertEquals(ABEntropy.get(0), ABEntropy.get(1));

    var uniformDistribution =
        IntStream.range(0, 1)
            .boxed()
            .collect(
                Collectors.toMap(
                    String::valueOf,
                    ignored ->
                        IntStream.range(0, 100)
                            .mapToObj(i -> 1.0 + i % 10 * 0.1)
                            .collect(Collectors.toUnmodifiableList())));
    // Since entropy represents the degree of uncertainty, uniform distribution does not represent
    // certainty in the concept of information entropy. On the contrary, because the uniform
    // distribution means that it is possible at every point, the degree of uncertainty is greater
    // and the weight given by the entropy method is also greater.
    uniformDistribution.put(
        "1",
        IntStream.range(0, 100).mapToObj(i -> 0.0 + i).collect(Collectors.toUnmodifiableList()));

    var uniformEntropy = weightProvider.weight(uniformDistribution);
    Assertions.assertTrue(uniformEntropy.get("0") < uniformEntropy.get("1"));
  }

  @ParameterizedTest
  @MethodSource("normalizers")
  void testEntropy(Normalizer normalizer) {
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
