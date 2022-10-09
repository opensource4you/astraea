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
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.gson.reflect.TypeToken;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.junit.jupiter.api.Test;

class JsonConverterTest {

  @Test
  void testMap() {
    var jsonConverter = JsonConverter.gson();
    var json = jsonConverter.toJson(Map.of("testKey", "testValue"));
    assertEquals("{\"testKey\":\"testValue\"}", json);

    Map<String, String> list =
        jsonConverter.fromJson(
            "{\"testKey\":\"testValue\"}", new TypeToken<Map<String, String>>() {}.getType());
    assertEquals(Map.of("testKey", "testValue"), list);
  }

  @Test
  void testList() {
    var jsonConverter = JsonConverter.gson();
    var json = jsonConverter.toJson(List.of("v1", "v2"));
    assertEquals("[\"v1\",\"v2\"]", json);

    List<String> list =
        jsonConverter.fromJson("[\"v1\",\"v2\"]", new TypeToken<List<String>>() {}.getType());
    assertEquals(List.of("v1", "v2"), list);
  }

  @Test
  void testSet() {
    var jsonConverter = JsonConverter.gson();
    var json = jsonConverter.toJson(Set.of("v1", "v2"));
    //    equals ignore order
    assertTrue("[\"v1\",\"v2\"]".equals(json) || "[\"v2\",\"v1\"]".equals(json));

    Set<String> set =
        jsonConverter.fromJson("[\"v1\",\"v2\"]", new TypeToken<Set<String>>() {}.getType());
    var expectedSet = Set.of("v1", "v2");
    //    equals ignore order
    assertTrue(expectedSet.containsAll(set) && set.containsAll(expectedSet));
  }

  @Test
  void testObject() {
    var jsonConverter = JsonConverter.gson();
    var json = jsonConverter.toJson(new TestClass("testString", 45678));
    assertEquals("{\"stringValue\":\"testString\",\"intValue\":45678}", json);

    var testObject =
        jsonConverter.fromJson(
            "{\"stringValue\":\"testString\",\"intValue\":45678}",
            new TypeToken<TestClass>() {}.getType());
    assertEquals(new TestClass("testString", 45678), testObject);

    testObject =
        jsonConverter.fromJson(
            "{\"stringValue\":\"testString\",\"intValue\":45678}", TestClass.class);
    assertEquals(new TestClass("testString", 45678), testObject);
  }

  private static class TestClass {
    private String stringValue;
    private int intValue;

    public TestClass(String stringValue, int intValue) {
      this.stringValue = stringValue;
      this.intValue = intValue;
    }

    public String stringValue() {
      return stringValue;
    }

    public void setStringValue(String stringValue) {
      this.stringValue = stringValue;
    }

    public int intValue() {
      return intValue;
    }

    public void setIntValue(int intValue) {
      this.intValue = intValue;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      TestClass testClass = (TestClass) o;
      return intValue == testClass.intValue && Objects.equals(stringValue, testClass.stringValue);
    }
  }
}
