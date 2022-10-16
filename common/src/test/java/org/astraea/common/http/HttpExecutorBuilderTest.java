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
package org.astraea.common.http;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import org.astraea.common.json.JsonConverter;
import org.astraea.common.json.TypeRef;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class HttpExecutorBuilderTest {

  @Test
  void testBuilder() {
    var builder = HttpExecutor.builder();
    assertEquals(JsonConverter.defaultConverter().getClass(), builder.jsonConverter.getClass());

    builder = HttpExecutor.builder().jsonConverter(new TestJsonConverter());
    assertEquals(TestJsonConverter.class, builder.jsonConverter.getClass());
  }

  @Test
  void testGetQueryUrl() throws URISyntaxException {
    var url = "http://localhost:8989/test";
    var newURL = HttpExecutorBuilder.getQueryUri(url, Map.of("key1", List.of("value1")));
    Assertions.assertEquals("key1=value1", newURL.getQuery());

    newURL = HttpExecutorBuilder.getQueryUri(url, Map.of("key1", List.of("value1", "value2")));
    Assertions.assertEquals("key1=value1,value2", newURL.getQuery());

    newURL = HttpExecutorBuilder.getQueryUri(url, Map.of("key1", List.of("/redirectKey")));
    Assertions.assertEquals("key1=%2FredirectKey", newURL.getQuery());

    newURL =
        HttpExecutorBuilder.getQueryUri(
            url, Map.of("key1", List.of("/redirectKey", "/redirectKey2")));
    Assertions.assertEquals("key1=%2FredirectKey,%2FredirectKey2", newURL.getQuery());
  }

  @Test
  void testConvertPojo2Parameter() {
    var testObj = new TestParameter();
    Map<String, List<String>> map = HttpExecutorBuilder.convert2Parameter(testObj);
    assertEquals(0, map.size());

    testObj = new TestParameter();
    testObj.doubleValue = 1.6;
    map = HttpExecutorBuilder.convert2Parameter(testObj);
    assertEquals(1, map.size());
    assertEquals("1.6", map.get("doubleValue").get(0));

    testObj = new TestParameter();
    testObj.doubleValue = 1.6;
    testObj.intValue = 5;
    testObj.stringValue = "hello";
    testObj.listStringValue = List.of("v1", "v2", "v3");
    testObj.listIntValue = List.of(50, 46, 12);
    map = HttpExecutorBuilder.convert2Parameter(testObj);
    assertEquals(5, map.size());
    assertEquals("1.6", map.get("doubleValue").get(0));
    assertEquals("5", map.get("intValue").get(0));
    assertEquals("hello", map.get("stringValue").get(0));
    assertEquals(List.of("v1", "v2", "v3"), map.get("listStringValue"));
    assertEquals(List.of("50", "46", "12"), map.get("listIntValue"));
  }

  @Test
  void testConvertMap2Parameter() {
    Map<String, List<String>> map = HttpExecutorBuilder.convert2Parameter(Map.of());
    assertEquals(0, map.size());

    map = HttpExecutorBuilder.convert2Parameter(Map.of("key", "value"));
    assertEquals(1, map.size());
    assertEquals("value", map.get("key").get(0));

    map = HttpExecutorBuilder.convert2Parameter(Map.of("key", List.of("v1", "v2")));
    assertEquals(1, map.size());
    assertEquals(List.of("v1", "v2"), map.get("key"));
  }

  static class TestParameter {
    private Integer intValue;
    private Double doubleValue;
    private String stringValue;
    private List<String> listStringValue;
    private List<Integer> listIntValue;
  }

  private static class TestJsonConverter implements JsonConverter {

    @Override
    public String toJson(Object src) {
      return null;
    }

    @Override
    public <T> T fromJson(String json, Class<T> tClass) {
      return null;
    }

    @Override
    public <T> T fromJson(String json, TypeRef<T> typeRef) {
      return null;
    }
  }
}
