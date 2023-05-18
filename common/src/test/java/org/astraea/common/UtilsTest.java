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

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.astraea.common.cost.CostFunction;
import org.astraea.common.cost.HasBrokerCost;
import org.astraea.common.cost.HasMoveCost;
import org.astraea.common.cost.RecordSizeCost;
import org.astraea.common.cost.ReplicaLeaderCost;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class UtilsTest {

  @Test
  void testShuffledPermutation() {
    var items = IntStream.range(0, 100000).boxed().toList();

    Assertions.assertEquals(
        100000,
        Utils.shuffledPermutation(items).stream().distinct().count(),
        "No duplicate element");
    Assertions.assertEquals(
        Set.copyOf(items),
        Utils.shuffledPermutation(items).stream().collect(Collectors.toSet()),
        "No element lost");
    Assertions.assertEquals(
        100000, Utils.shuffledPermutation(items).stream().count(), "Size correct");
    Assertions.assertNotEquals(
        items, Utils.shuffledPermutation(items).stream().toList(), "Content randomized");
  }

  @Test
  void testShuffledPermutationStatistics() {
    // Generate 10 elements, perform the shuffle multiple time and counting the number frequency of
    // each position. See if under large number of experiments, the number is uniformly distributed.
    var items = IntStream.range(0, 10).boxed().toList();
    var buckets = new int[10][10];

    var trials = 100000;
    for (int i = 0; i < trials; i++) {
      var index = 0;
      for (int x : Utils.shuffledPermutation(items)) {
        buckets[x][index++] += 1;
      }
    }

    var expectedValue = trials / 10.0;
    var error = 1.0;
    var result =
        IntStream.range(0, buckets.length)
            .boxed()
            .collect(
                Collectors.toUnmodifiableMap(
                    i -> i, i -> Arrays.stream(buckets[i]).boxed().toList()));
    Assertions.assertTrue(
        result.values().stream()
            .map(x -> x.stream().mapToInt(i -> i).average().orElse(0))
            .allMatch(x -> expectedValue - error <= x && x <= expectedValue + error),
        "The implementation might be biased: " + result);
  }

  @Test
  void testChunk() {
    var input = List.of("a", "b", "c", "d");

    var output = List.copyOf(Utils.chunk(input, 1));
    Assertions.assertEquals(1, output.size());
    Assertions.assertEquals(input, output.get(0));

    var output2 = List.copyOf(Utils.chunk(input, 2));
    Assertions.assertEquals(2, output2.size());
    Assertions.assertEquals(List.of("a", "c"), output2.get(0));
    Assertions.assertEquals(List.of("b", "d"), output2.get(1));

    var output3 = List.copyOf(Utils.chunk(input, 3));
    Assertions.assertEquals(3, output3.size(), output3.toString());
    Assertions.assertEquals(List.of("a", "d"), output3.get(0));
    Assertions.assertEquals(List.of("b"), output3.get(1));
    Assertions.assertEquals(List.of("c"), output3.get(2));
  }

  @Test
  void testHandleException() {
    var executionRuntimeException =
        Assertions.assertThrows(
            ExecutionRuntimeException.class,
            () ->
                Utils.packException(
                    () -> {
                      throw new ExecutionException(new IllegalArgumentException());
                    }));

    Assertions.assertEquals(
        IllegalArgumentException.class, executionRuntimeException.getRootCause().getClass());

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            Utils.packException(
                () -> {
                  throw new IllegalArgumentException();
                }));

    Assertions.assertThrows(
        RuntimeException.class,
        () ->
            Utils.packException(
                () -> {
                  throw new IOException();
                }));
  }

  @Test
  void testCollectToTreeMap() {
    Assertions.assertInstanceOf(
        SortedMap.class,
        IntStream.range(0, 100).boxed().collect(MapUtils.toSortedMap(i -> i, i -> i)));
    //noinspection ResultOfMethodCallIgnored
    Assertions.assertThrows(
        IllegalStateException.class,
        () ->
            Stream.of(Map.entry(1, "hello"), Map.entry(1, "world"))
                .collect(MapUtils.toSortedMap(Map.Entry::getKey, Map.Entry::getValue)));
  }

  @Test
  void testSequence() {
    var future1 = CompletableFuture.supplyAsync(() -> 1);
    var future2 = CompletableFuture.supplyAsync(() -> 2);

    Assertions.assertEquals(FutureUtils.sequence(List.of(future1, future2)).join(), List.of(1, 2));
  }

  @Test
  void testNonEmpty() {
    Assertions.assertThrows(IllegalArgumentException.class, () -> Utils.requireNonEmpty(""));
    Assertions.assertThrows(NullPointerException.class, () -> Utils.requireNonEmpty(null));
  }

  @Test
  void testSwallowException() {
    Assertions.assertDoesNotThrow(
        () ->
            Utils.swallowException(
                () -> {
                  throw new IllegalArgumentException();
                }));
  }

  @Test
  void testMember() {
    var dumb = new Dumb(100);
    var value = (int) Utils.member(dumb, "value");
    Assertions.assertEquals(dumb.value, value);
  }

  @Test
  void testStaticMember() {
    Assertions.assertEquals(Dumb.CONSTANT, Utils.staticMember(Dumb.class, "CONSTANT"));
  }

  private static class Dumb {
    private static final int CONSTANT = 1000;

    private final int value;

    Dumb(int v) {
      this.value = v;
    }

    public int value() {
      return value;
    }
  }

  @ParameterizedTest
  @ValueSource(classes = {TestCostFunction.class, TestConfigCostFunction.class})
  void testConstruct(Class<? extends CostFunction> aClass) {
    var config = new Configuration(Map.of());

    var costFunction = Utils.construct(aClass, config);
    Assertions.assertInstanceOf(CostFunction.class, costFunction);
    Assertions.assertInstanceOf(aClass, costFunction);

    var costFunction2 = Utils.construct(aClass.getName(), CostFunction.class, config);
    Assertions.assertInstanceOf(CostFunction.class, costFunction2);
    Assertions.assertInstanceOf(aClass, costFunction2);

    // Cost function class can't be cast to String class
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> Utils.construct(aClass.getName(), String.class, config));
  }

  @Test
  void testConstructException() {
    // arrange
    var aClass = TestBadCostFunction.class;
    var config = new Configuration(Map.of());

    // act, assert
    Assertions.assertThrows(RuntimeException.class, () -> Utils.construct(aClass, config));
  }

  @Test
  void testEmptySequence() {
    var f = FutureUtils.sequence(List.of()).thenApply(ignored -> "yes");
    Assertions.assertEquals("yes", f.join());
  }

  @Test
  void testWildcard() {
    var pattern0 = Utils.wildcardToPattern("aaa*");
    Assertions.assertTrue(pattern0.matcher("aaa").matches());
    Assertions.assertTrue(pattern0.matcher("aaab").matches());
    Assertions.assertTrue(pattern0.matcher("aaacc").matches());
    Assertions.assertFalse(pattern0.matcher("bbaaa").matches());
    Assertions.assertFalse(pattern0.matcher("ccaaadd").matches());
    Assertions.assertFalse(pattern0.matcher("aa").matches());

    var pattern1 = Utils.wildcardToPattern("*aaa*");
    Assertions.assertTrue(pattern1.matcher("aaa").matches());
    Assertions.assertTrue(pattern1.matcher("aaab").matches());
    Assertions.assertTrue(pattern1.matcher("aaacc").matches());
    Assertions.assertTrue(pattern1.matcher("bbaaa").matches());
    Assertions.assertTrue(pattern1.matcher("ccaaadd").matches());
    Assertions.assertFalse(pattern1.matcher("aa").matches());

    var pattern2 = Utils.wildcardToPattern("?aaa*");
    Assertions.assertFalse(pattern2.matcher("aaa").matches());
    Assertions.assertFalse(pattern2.matcher("aaab").matches());
    Assertions.assertFalse(pattern2.matcher("aaacc").matches());
    Assertions.assertTrue(pattern2.matcher("baaa").matches());
    Assertions.assertTrue(pattern2.matcher("caaadd").matches());
    Assertions.assertFalse(pattern2.matcher("aa").matches());

    var pattern3 = Utils.wildcardToPattern("192*");
    Assertions.assertTrue(pattern3.matcher("192.168").matches());
  }

  private static class TestConfigCostFunction implements CostFunction {
    public TestConfigCostFunction(Configuration configuration) {}
  }

  private static class TestCostFunction implements CostFunction {
    public TestCostFunction() {}
  }

  private static class TestBadCostFunction implements CostFunction {
    public TestBadCostFunction(int value) {}
  }

  @Test
  void testCostFunctions() {
    var config =
        new Configuration(
            Map.of(
                "org.astraea.common.cost.BrokerInputCost",
                "20",
                "org.astraea.common.cost.BrokerOutputCost",
                "1.25"));
    var ans = Utils.costFunctions(config.raw(), HasBrokerCost.class, config);
    Assertions.assertEquals(2, ans.size());
    for (var entry : ans.entrySet()) {
      if (entry.getKey().getClass().getName().equals("org.astraea.common.cost.BrokerInputCost")) {
        Assertions.assertEquals(20.0, entry.getValue());
      } else if (entry
          .getKey()
          .getClass()
          .getName()
          .equals("org.astraea.common.cost.BrokerOutputCost")) {
        Assertions.assertEquals(1.25, entry.getValue());
      } else {
        Assertions.assertEquals(0.0, entry.getValue());
      }
    }

    // test negative weight
    var config2 =
        new Configuration(
            Map.of(
                "org.astraea.common.cost.BrokerInputCost",
                "-20",
                "org.astraea.common.cost.BrokerOutputCost",
                "1.25"));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> Utils.costFunctions(config2.raw(), HasBrokerCost.class, config2));

    // test moveCost
    var cf =
        Set.of(
            "org.astraea.common.cost.RecordSizeCost", "org.astraea.common.cost.ReplicaLeaderCost");
    var mConfig = new Configuration(Map.of("maxMigratedSize", "50MB", "maxMigratedLeader", "5"));
    var mAns = Utils.costFunctions(cf, HasMoveCost.class, mConfig);
    Assertions.assertEquals(2, mAns.size());

    // test configs
    Assertions.assertEquals(
        "50MB",
        mAns.stream()
            .filter(c -> c instanceof RecordSizeCost)
            .map(c -> (RecordSizeCost) c)
            .findFirst()
            .get()
            .config()
            .string("maxMigratedSize")
            .get());

    Assertions.assertEquals(
        "5",
        mAns.stream()
            .filter(c -> c instanceof ReplicaLeaderCost)
            .map(c -> (ReplicaLeaderCost) c)
            .findFirst()
            .get()
            .config()
            .string("maxMigratedLeader")
            .get());
  }

  @Test
  void testClose() {
    Assertions.assertDoesNotThrow(() -> Utils.close(null));
    var count = new AtomicInteger();
    Closeable obj = count::incrementAndGet;
    Assertions.assertDoesNotThrow(() -> Utils.close(obj));
    Assertions.assertEquals(1, count.get());
  }

  @Test
  void testRequireNonEmpty() {
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> Utils.requireNonEmpty(Set.of(), ""));
    Assertions.assertDoesNotThrow(() -> Utils.requireNonEmpty(List.of(1), ""));
  }
}
