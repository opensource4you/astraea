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
package org.astraea.app.web;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.astraea.common.json.TypeRef;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class PostRequestTest {

  @Test
  void testOfEmptyJson() {
    var postRequest = PostRequest.of("{}");
    var map = postRequest.getRequest(TypeRef.of(Map.class));
    Assertions.assertEquals(0, map.size());
  }

  @Test
  void testGetRequestObject() {
    var postRequest = PostRequest.of("{\"foo\":\"fooValue\",\"bar\":500}");
    var forTestValue = postRequest.getRequest(TypeRef.of(ForTestValue.class));
    Assertions.assertEquals("fooValue", forTestValue.foo);
    Assertions.assertEquals(500, forTestValue.bar);

    var errPostRequest = PostRequest.of("{\"foo\":\"fooValue\"}");
    var exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> errPostRequest.getRequest(TypeRef.of(ForTestValue.class)));
    Assertions.assertEquals("Value `$.bar` is required.", exception.getMessage());
  }

  @Test
  void testGetRequestNested() {

    var postRequest = PostRequest.of("{\"forTestValue\":{\"foo\":\"fooValue\",\"bar\":500}}");
    var forTestNested = postRequest.getRequest(TypeRef.of(ForTestNested.class));
    Assertions.assertEquals("fooValue", forTestNested.forTestValue.foo);
    Assertions.assertEquals(500, forTestNested.forTestValue.bar);

    var errNestedPostRequest = PostRequest.of("{\"forTestValue\":{\"foo\":\"fooValue\"}}");
    var nestedException =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> errNestedPostRequest.getRequest(TypeRef.of(ForTestNested.class)));
    Assertions.assertEquals(
        "Value `$.forTestValue.bar` is required.", nestedException.getMessage());
  }

  @Test
  void testGetRequestList() {
    var postRequest = PostRequest.of("{\"forTestValues\":[{\"foo\":\"fooValue\",\"bar\":500}]}");
    var forTestList = postRequest.getRequest(TypeRef.of(ForTestList.class));
    Assertions.assertEquals("fooValue", forTestList.forTestValues.get(0).foo);
    Assertions.assertEquals(500, forTestList.forTestValues.get(0).bar);

    var errListPostRequest = PostRequest.of("{\"forTestValues\":[{\"foo\":\"fooValue\"}]}");
    var listException =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> errListPostRequest.getRequest(TypeRef.of(ForTestList.class)));
    Assertions.assertEquals(
        "Value `$.forTestValues[].bar` is required.", listException.getMessage());
  }

  @Test
  void testGetRequestMap() {
    var postRequest =
        PostRequest.of("{\"forTestValueMap\":{\"mapKey\":{\"foo\":\"fooValue\",\"bar\":500}}}");
    var forTestMap = postRequest.getRequest(TypeRef.of(ForTestMap.class));
    Assertions.assertEquals("fooValue", forTestMap.forTestValueMap.get("mapKey").foo);
    Assertions.assertEquals(500, forTestMap.forTestValueMap.get("mapKey").bar);

    var errMapPostRequest =
        PostRequest.of("{\"forTestValueMap\":{\"mapKey\":{\"foo\":\"fooValue\"}}}");
    var mapException =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> errMapPostRequest.getRequest(TypeRef.of(ForTestMap.class)));
    Assertions.assertEquals(
        "Value `$.forTestValueMap{}.bar` is required.", mapException.getMessage());
  }

  @Test
  void testGetRequestOptional() {
    var postRequest = PostRequest.of("{\"forTestValue\":{\"foo\":\"fooValue\",\"bar\":500}}");
    var forTestOptional = postRequest.getRequest(TypeRef.of(ForTestOptional.class));
    Assertions.assertEquals("fooValue", forTestOptional.forTestValue.get().foo);
    Assertions.assertEquals(500, forTestOptional.forTestValue.get().bar);

    var errOptionalPostRequest = PostRequest.of("{\"forTestValue\":{\"foo\":\"fooValue\"}}");
    var optionalException =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> errOptionalPostRequest.getRequest(TypeRef.of(ForTestOptional.class)));
    Assertions.assertEquals(
        "Value `$.forTestValue.bar` is required.", optionalException.getMessage());
  }

  @Test
  void testConvert() {
    var postRequest = PostRequest.of("{\"foo\":\"fooValue\",\"bar\":500}");
    var obj = postRequest.getRequest(TypeRef.of(Object.class));

    var forTestValue = PostRequest.convert(obj, TypeRef.of(ForTestValue.class));
    Assertions.assertEquals("fooValue", forTestValue.foo);
    Assertions.assertEquals(500, forTestValue.bar);

    var errPostRequest = PostRequest.of("{\"foo\":\"fooValue\"}");
    var errObj = errPostRequest.getRequest(TypeRef.of(Object.class));
    var exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> PostRequest.convert(errObj, TypeRef.of(ForTestValue.class)));
    Assertions.assertEquals("Value `$.bar` is required.", exception.getMessage());
  }

  static class ForTestOptional {
    Optional<ForTestValue> forTestValue = Optional.empty();
  }

  static class ForTestMap {
    Map<String, ForTestValue> forTestValueMap;
  }

  static class ForTestList {
    List<ForTestValue> forTestValues;
  }

  static class ForTestNested {
    ForTestValue forTestValue;
  }

  static class ForTestValue {
    String foo;
    Integer bar;

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if ((obj == null) || (getClass() != obj.getClass())) {
        return false;
      }
      ForTestValue other = (ForTestValue) obj;
      return Objects.equals(foo, other.foo) && Objects.equals(bar, other.bar);
    }
  }
}
