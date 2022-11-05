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
package org.astraea.common.json;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;

class JsonConverterTest {

  private JsonConverter getConverter() {
    return JsonConverter.jackson();
  }

  @Test
  void testMap() {
    var jsonConverter = getConverter();
    var json = jsonConverter.toJson(Map.of("testKey", "testValue"));
    assertEquals("{\"testKey\":\"testValue\"}", json);

    Map<String, String> list =
        jsonConverter.fromJson("{\"testKey\":\"testValue\"}", new TypeRef<>() {});
    assertEquals(Map.of("testKey", "testValue"), list);
  }

  @Test
  void testList() {
    var jsonConverter = getConverter();
    var json = jsonConverter.toJson(List.of("v1", "v2"));
    assertEquals("[\"v1\",\"v2\"]", json);

    List<String> list = jsonConverter.fromJson("[\"v1\",\"v2\"]", new TypeRef<>() {});
    assertEquals(List.of("v1", "v2"), list);
  }

  @Test
  void testSet() {
    var jsonConverter = getConverter();
    var json = jsonConverter.toJson(Set.of("v1", "v2"));
    //    equals ignore order
    assertTrue("[\"v1\",\"v2\"]".equals(json) || "[\"v2\",\"v1\"]".equals(json));

    Set<String> set = jsonConverter.fromJson("[\"v1\",\"v2\"]", new TypeRef<>() {});
    var expectedSet = Set.of("v1", "v2");
    //    equals ignore order
    assertTrue(expectedSet.containsAll(set) && set.containsAll(expectedSet));
  }

  @Test
  void testPrimitive() {
    var jsonConverter = getConverter();
    var testFieldClass = new TestPrimitiveClass();
    testFieldClass.doubleValue = 456d;
    testFieldClass.intValue = 12;
    testFieldClass.stringValue = "hello";

    var json = jsonConverter.toJson(testFieldClass);
    assertEquals("{\"doubleValue\":456.0,\"intValue\":12,\"stringValue\":\"hello\"}", json);

    var convertedTestFieldClass =
        jsonConverter.fromJson(
            "{\"doubleValue\":456.0,\"intValue\":12,\"stringValue\":\"hello\"}",
            TypeRef.of(TestPrimitiveClass.class));
    assertEquals(456d, convertedTestFieldClass.doubleValue);
    assertEquals(12, convertedTestFieldClass.intValue);
    assertEquals("hello", convertedTestFieldClass.stringValue);
  }

  /**
   * In normal json-deserialization lib, if the field is not existed in json, it doesn't run the
   * deserialize process. So we need to set Optional opt = Optional.empty()
   */
  @Test
  void testOptional() {
    var jsonConverter = getConverter();
    var testFieldClass = new TestOptionalClass();
    testFieldClass.optValue = Optional.ofNullable("hello");
    testFieldClass.nestedOpt = Optional.ofNullable(List.of("hello"));
    testFieldClass.nonInitOpt = Optional.ofNullable("hello");

    var json = jsonConverter.toJson(testFieldClass);
    assertEquals(
        "{\"nestedOpt\":[\"hello\"],\"nonInitOpt\":\"hello\",\"optValue\":\"hello\"}", json);

    testFieldClass.optValue = Optional.empty();
    testFieldClass.nestedOpt = Optional.empty();
    testFieldClass.nonInitOpt = Optional.empty();
    json = jsonConverter.toJson(testFieldClass);
    assertEquals("{}", json);

    var convertedTestFieldClass =
        jsonConverter.fromJson(
            "{\"nestedOpt\":[\"hello\"],\"nonInitOpt\":\"hello\",\"optValue\":\"hello\"}",
            TypeRef.of(TestOptionalClass.class));
    assertEquals("hello", convertedTestFieldClass.optValue.get());
    assertEquals(List.of("hello"), convertedTestFieldClass.nestedOpt.get());
    assertEquals("hello", convertedTestFieldClass.nonInitOpt.get());

    convertedTestFieldClass =
        jsonConverter.fromJson("{\"optValue\":null}", TypeRef.of(TestOptionalClass.class));
    assertTrue(convertedTestFieldClass.optValue.isEmpty());
    assertTrue(convertedTestFieldClass.nestedOpt.isEmpty());
    assertNull(convertedTestFieldClass.nonInitOpt);
  }

  @Test
  void testNestedObject() {
    var jsonConverter = getConverter();

    var testNestedObjectClass = new TestNestedObjectClass();
    testNestedObjectClass.nestedList = List.of(Map.of("key", "value"));
    testNestedObjectClass.nestedObject = new TestPrimitiveClass();
    testNestedObjectClass.nestedMap = Map.of("helloKey", List.of("hello"));

    var json = jsonConverter.toJson(testNestedObjectClass);
    var expectedJson =
        "{\"nestedList\":[{\"key\":\"value\"}],\"nestedMap\":{\"helloKey\":[\"hello\"]},\"nestedObject\":{\"intValue\":0}}";
    assertEquals(expectedJson, json);

    var fromJson = jsonConverter.fromJson(expectedJson, TypeRef.of(TestNestedObjectClass.class));
    assertEquals("value", fromJson.nestedList.get(0).get("key"));
    assertEquals(0, fromJson.nestedObject.intValue);
    assertEquals("hello", fromJson.nestedMap.get("helloKey").get(0));
  }

  @Test
  void testToObjectIgnoreOrder() {
    var jsonConverter = getConverter();
    var json = "{\"doubleValue\":456.0,\"intValue\":12,\"stringValue\":\"hello\"}";
    var sameJsonDiffOrder = "{\"stringValue\":\"hello\",\"doubleValue\":456.0,\"intValue\":12}";

    var convertedTestFieldClass =
        jsonConverter.fromJson(json, TypeRef.of(TestPrimitiveClass.class));
    assertEquals(456d, convertedTestFieldClass.doubleValue);
    assertEquals(12, convertedTestFieldClass.intValue);
    assertEquals("hello", convertedTestFieldClass.stringValue);

    convertedTestFieldClass =
        jsonConverter.fromJson(sameJsonDiffOrder, TypeRef.of(TestPrimitiveClass.class));
    assertEquals(456d, convertedTestFieldClass.doubleValue);
    assertEquals(12, convertedTestFieldClass.intValue);
    assertEquals("hello", convertedTestFieldClass.stringValue);
  }

  @Test
  void testTrim() {
    var jsonConverter = getConverter();
    var testFieldNameClass = new TestFieldNameClass();
    testFieldNameClass.beta = List.of("notMatter");
    testFieldNameClass.banana = "notMatter";
    testFieldNameClass.apple = "notMatter";
    testFieldNameClass.actor = 123;
    testFieldNameClass.dog = new TestPrimitiveClass();
    var json = jsonConverter.toJson(testFieldNameClass);
    assertEquals(json, json.trim());
  }

  @Test
  void testFieldNameOrder() {
    var jsonConverter = getConverter();
    var testFieldNameClass = new TestFieldNameClass();
    testFieldNameClass.beta = List.of("notMatter");
    testFieldNameClass.banana = "notMatter";
    testFieldNameClass.apple = "notMatter";
    testFieldNameClass.actor = 123;
    testFieldNameClass.dog = new TestPrimitiveClass();

    var json = jsonConverter.toJson(testFieldNameClass);
    assertEquals(
        "{\"actor\":123,"
            + "\"apple\":\"notMatter\","
            + "\"banana\":\"notMatter\","
            + "\"beta\":[\"notMatter\"],"
            + "\"dog\":{\"intValue\":0}"
            + "}",
        json);
  }

  /** String Equal as same as Bytes Equal */
  @Test
  void testJsonStringEquals() {
    var jsonConverter = getConverter();
    var v0 = new V0();
    var v1 = new V1();
    assertEquals("{\"a\":123,\"b\":345}", jsonConverter.toJson(v0));
    assertEquals("{\"a\":123,\"b\":345}", jsonConverter.toJson(v1));
    assertEquals(jsonConverter.toJson(v0), jsonConverter.toJson(v1));
  }

  @Test
  void testSerializeMapEquals() {
    var jsonConverter = getConverter();
    assertEquals("{\"a\":\"b\",\"c\":\"d\"}", jsonConverter.toJson(Map.of("a", "b", "c", "d")));
    assertEquals("{\"a\":\"b\",\"c\":\"d\"}", jsonConverter.toJson(Map.of("c", "d", "a", "b")));
  }

  @Test
  void testFieldNotInJson() {
    var testFieldNameClass =
        JsonConverter.defaultConverter()
            .fromJson(
                "{" + "\"actor\":123," + "\"apple\":\"notMatter\"" + "}",
                TypeRef.of(TestFieldNameClass.class));

    assertEquals(123, testFieldNameClass.actor);
    assertEquals("notMatter", testFieldNameClass.apple);
    assertNull(testFieldNameClass.banana);
  }

  private static class V0 {
    int a = 123;
    int b = 345;
  }

  private static class V1 {
    int b = 345;
    int a = 123;
  }

  /** order should be actor, apple, banana, beta, dog */
  private static class TestFieldNameClass {

    private List<String> beta;
    private String banana;
    private String apple;
    private int actor;
    private TestPrimitiveClass dog;
  }

  private static class TestOptionalClass {

    private Optional<String> optValue = Optional.empty();
    private Optional<List<String>> nestedOpt = Optional.empty();

    /**
     * if opt is not initialized with Optional.empty(), nonInitOpt will be null when nonInitOpt is
     * not in json fields.
     */
    private Optional<String> nonInitOpt;
  }

  private static class TestNestedObjectClass {

    private List<Map<String, String>> nestedList;
    private Map<String, List<String>> nestedMap;
    private TestPrimitiveClass nestedObject;
  }

  private static class TestPrimitiveClass {

    private String stringValue;
    private int intValue;
    private Double doubleValue;
  }
}
