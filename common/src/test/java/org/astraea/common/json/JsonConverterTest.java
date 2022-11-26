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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Assertions;
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

    var json = jsonConverter.toJson(testFieldClass);
    assertEquals("{\"nestedOpt\":[\"hello\"],\"optValue\":\"hello\"}", json);

    testFieldClass.optValue = Optional.empty();
    testFieldClass.nestedOpt = Optional.empty();
    json = jsonConverter.toJson(testFieldClass);
    assertEquals("{}", json);

    var convertedTestFieldClass =
        jsonConverter.fromJson(
            "{\"nestedOpt\":[\"hello\"],\"optValue\":\"hello\"}",
            TypeRef.of(TestOptionalClass.class));
    assertEquals("hello", convertedTestFieldClass.optValue.get());
    assertEquals(List.of("hello"), convertedTestFieldClass.nestedOpt.get());

    convertedTestFieldClass =
        jsonConverter.fromJson("{\"optValue\":null}", TypeRef.of(TestOptionalClass.class));
    assertTrue(convertedTestFieldClass.optValue.isEmpty());
    assertTrue(convertedTestFieldClass.nestedOpt.isEmpty());
  }

  @Test
  void testNestedObject() {
    var jsonConverter = getConverter();

    var testNestedObjectClass = new TestNestedObjectClass();
    testNestedObjectClass.nestedList = List.of(Map.of("key", "value"));
    testNestedObjectClass.nestedObject = new TestPrimitiveClass();
    testNestedObjectClass.nestedObject.doubleValue = 5d;
    testNestedObjectClass.nestedObject.stringValue = "value";
    testNestedObjectClass.nestedMap = Map.of("helloKey", List.of("hello"));

    var json = jsonConverter.toJson(testNestedObjectClass);
    var expectedJson =
        "{\"nestedList\":[{\"key\":\"value\"}],\"nestedMap\":{\"helloKey\":[\"hello\"]},\"nestedObject\":{\"doubleValue\":5.0,\"intValue\":0,\"stringValue\":\"value\"}}";
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
  void testFieldNotInJson() throws JsonProcessingException {
    var testPrimitiveClass =
        getConverter()
            .fromJson(
                "{\"doubleValue\":5.0,\"intValue\":0,\"stringValue\":\"value\",\"notInField\":\"value\"}",
                TypeRef.of(TestPrimitiveClass.class));

    assertEquals(5.0d, testPrimitiveClass.doubleValue);
    assertEquals(0, testPrimitiveClass.intValue);
    assertEquals("value", testPrimitiveClass.stringValue);
  }

  @Test
  void testByteArray() {
    var foo = new Foo();
    foo.bar = "test".getBytes();
    var jsonConverter = getConverter();
    Assertions.assertArrayEquals(
        "test".getBytes(),
        jsonConverter.fromJson(jsonConverter.toJson(foo), TypeRef.of(Foo.class)).bar);
  }

  @Test
  void testNullObject() {
    var jsonConverter = getConverter();
    var forTestValue =
        jsonConverter.fromJson(
            "{\"foo\":\"fooValue\",\"bar\":500}", TypeRef.of(ForTestConvert.class));
    Assertions.assertEquals("fooValue", forTestValue.foo);
    Assertions.assertEquals(500, forTestValue.bar);

    var exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                jsonConverter.fromJson("{\"foo\":\"fooValue\"}", TypeRef.of(ForTestConvert.class)));
    Assertions.assertEquals("$.bar can not be null", exception.getMessage());
  }

  @Test
  void testNullNested() {
    var jsonConverter = getConverter();
    var forTestNested =
        jsonConverter.fromJson(
            "{\"forTestValue\":{\"foo\":\"fooValue\",\"bar\":500}}",
            TypeRef.of(ForTestNested.class));
    Assertions.assertEquals("fooValue", forTestNested.forTestValue.foo);
    Assertions.assertEquals(500, forTestNested.forTestValue.bar);

    var exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                jsonConverter.fromJson(
                    "{\"forTestValue\":{\"foo\":\"fooValue\"}}", TypeRef.of(ForTestNested.class)));
    Assertions.assertEquals("$.forTestValue.bar can not be null", exception.getMessage());
  }

  @Test
  void testNullList() {
    var jsonConverter = getConverter();
    var forTestList =
        jsonConverter.fromJson(
            "{\"forTestValues\":[{\"foo\":\"fooValue\",\"bar\":500}]}",
            TypeRef.of(ForTestList.class));
    Assertions.assertEquals("fooValue", forTestList.forTestValues.get(0).foo);
    Assertions.assertEquals(500, forTestList.forTestValues.get(0).bar);

    var exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                jsonConverter.fromJson(
                    "{\"forTestValues\":[{\"foo\":\"fooValue\"}]}", TypeRef.of(ForTestList.class)));
    Assertions.assertEquals("$.forTestValues[0].bar can not be null", exception.getMessage());
  }

  @Test
  void testNullMap() {
    var jsonConverter = getConverter();
    var forTestMap =
        jsonConverter.fromJson(
            "{\"forTestValueMap\":{\"mapKey\":{\"foo\":\"fooValue\",\"bar\":500}}}",
            TypeRef.of(ForTestMap.class));
    Assertions.assertEquals("fooValue", forTestMap.forTestValueMap.get("mapKey").foo);
    Assertions.assertEquals(500, forTestMap.forTestValueMap.get("mapKey").bar);

    var exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                jsonConverter.fromJson(
                    "{\"forTestValueMap\":{\"mapKey\":{\"foo\":\"fooValue\"}}}",
                    TypeRef.of(ForTestMap.class)));
    Assertions.assertEquals("$.forTestValueMap.mapKey.bar can not be null", exception.getMessage());
  }

  @Test
  void testNullOptional() {
    var jsonConverter = getConverter();
    var forTestOptional =
        jsonConverter.fromJson(
            "{\"forTestValue\":{\"foo\":\"fooValue\",\"bar\":500}}",
            TypeRef.of(ForTestOptional.class));
    Assertions.assertEquals("fooValue", forTestOptional.forTestValue.get().foo);
    Assertions.assertEquals(500, forTestOptional.forTestValue.get().bar);

    var exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                jsonConverter.fromJson(
                    "{\"forTestValue\":{\"foo\":\"fooValue\"}}",
                    TypeRef.of(ForTestOptional.class)));
    Assertions.assertEquals("$.forTestValue.bar can not be null", exception.getMessage());
  }

  @Test
  void testGenericDefault() {
    var jsonConverter = getConverter();
    var forTestDefault = jsonConverter.fromJson("{}", TypeRef.of(ForTestDefault.class));

    assertNotNull(forTestDefault.list);
    assertNotNull(forTestDefault.map);
    assertNotNull(forTestDefault.opt);

    forTestDefault =
        jsonConverter.fromJson(
            "{\"list\":null,\"map\":null,\"opt\":null}", TypeRef.of(ForTestDefault.class));
    assertNotNull(forTestDefault.list);
    assertNotNull(forTestDefault.map);
    assertNotNull(forTestDefault.opt);
  }

  static class ForTestDefault {
    Optional<ForTestConvert> opt = Optional.empty();
    Map<String, ForTestConvert> map = Map.of();
    List<ForTestConvert> list = List.of();
  }

  static class ForTestOptional {

    Optional<ForTestConvert> forTestValue = Optional.empty();
  }

  static class ForTestMap {

    Map<String, ForTestConvert> forTestValueMap;
  }

  static class ForTestList {

    List<ForTestConvert> forTestValues;
  }

  static class ForTestNested {

    ForTestConvert forTestValue;
  }

  static class ForTestConvert {
    String foo;
    Integer bar;
  }

  private static class Foo {
    byte[] bar;
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

    /**
     * if opt is not initialized with Optional.empty(), it will be null when nonInitOpt is not in
     * json fields.
     */
    private Optional<String> optValue = Optional.empty();

    private Optional<List<String>> nestedOpt = Optional.empty();
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
