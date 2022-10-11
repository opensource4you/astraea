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

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.astraea.common.cost.Configuration;
import org.astraea.common.cost.CostFunction;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class UtilsTest {

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
        IntStream.range(0, 100).boxed().collect(Utils.toSortedMap(i -> i, i -> i)));
    //noinspection ResultOfMethodCallIgnored
    Assertions.assertThrows(
        IllegalStateException.class,
        () ->
            Stream.of(Map.entry(1, "hello"), Map.entry(1, "world"))
                .collect(Utils.toSortedMap(Map.Entry::getKey, Map.Entry::getValue)));
  }

  @Test
  void testSequence() {
    var future1 = CompletableFuture.supplyAsync(() -> 1);
    var future2 = CompletableFuture.supplyAsync(() -> 2);

    Assertions.assertEquals(Utils.sequence(List.of(future1, future2)).join(), List.of(1, 2));
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

  @Test
  void testGetQueryUrl() throws URISyntaxException {
    var url = "http://localhost:8989/test";
    var newURL = Utils.getQueryUri(url, Map.of("key1", "value1"));
    Assertions.assertEquals("key1=value1", newURL.getQuery());

    newURL = Utils.getQueryUri(url, Map.of("key1", "value1,value2"));
    Assertions.assertEquals("key1=value1,value2", newURL.getQuery());

    newURL = Utils.getQueryUri(url, Map.of("key1", "/redirectKey"));
    Assertions.assertEquals("key1=%2FredirectKey", newURL.getQuery());

    newURL = Utils.getQueryUri(url, Map.of("key1", "/redirectKey,/redirectKey2"));
    Assertions.assertEquals("key1=%2FredirectKey,%2FredirectKey2", newURL.getQuery());
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
    // arrange
    var config = Configuration.of(Map.of());

    // act
    var costFunction = Utils.construct(aClass, config);

    // assert
    Assertions.assertInstanceOf(CostFunction.class, costFunction);
    Assertions.assertInstanceOf(aClass, costFunction);
  }

  @Test
  void testConstructException() {
    // arrange
    var aClass = TestBadCostFunction.class;
    var config = Configuration.of(Map.of());

    // act, assert
    Assertions.assertThrows(RuntimeException.class, () -> Utils.construct(aClass, config));
  }

  @Test
  void testEmptySequence() throws ExecutionException, InterruptedException {
    var f = Utils.sequence(List.of()).thenApply(ignored -> "yes");
    Assertions.assertEquals("yes", f.get());
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
}
