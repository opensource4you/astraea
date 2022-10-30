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

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class TypeRefTest {

  @Test
  void testGeneric() {
    var stringType = new TypeRef<String>() {};
    var listType = new TypeRef<List<String>>() {};
    var mapType = new TypeRef<Map<String, Integer>>() {};

    assertEquals("class java.lang.String", stringType.getType().toString());
    assertEquals("java.util.List<java.lang.String>", listType.getType().toString());
    assertEquals(
        "java.util.Map<java.lang.String, java.lang.Integer>", mapType.getType().toString());
  }

  @Test
  void testOf() {
    var existedType = new TypeRef<List<String>>() {}.getType();
    assertEquals("java.util.List<java.lang.String>", TypeRef.of(existedType).getType().toString());
    assertEquals("class java.lang.String", TypeRef.of(String.class).getType().toString());
  }

  @Test
  void testArray() {
    assertEquals(
        "java.util.List<java.lang.String>", TypeRef.array(String.class).getType().toString());
    assertEquals(
        "java.util.List<java.lang.Integer>", TypeRef.array(Integer.class).getType().toString());
  }

  @Test
  void testMap() {
    assertEquals(
        "java.util.Map<java.lang.String, java.lang.String>",
        TypeRef.map(String.class).getType().toString());
    assertEquals(
        "java.util.Map<java.lang.String, java.lang.Integer>",
        TypeRef.map(Integer.class).getType().toString());
  }

  @Test
  void testSet() {
    assertEquals("java.util.Set<java.lang.String>", TypeRef.set(String.class).getType().toString());
    assertEquals(
        "java.util.Set<java.lang.Integer>", TypeRef.set(Integer.class).getType().toString());
  }

  @Test
  void testEquals() {
    assertEquals(TypeRef.set(String.class), TypeRef.set(String.class));
    assertEquals(TypeRef.map(Integer.class), TypeRef.map(Integer.class));
    assertEquals(TypeRef.array(Integer.class), TypeRef.array(Integer.class));
  }
}
