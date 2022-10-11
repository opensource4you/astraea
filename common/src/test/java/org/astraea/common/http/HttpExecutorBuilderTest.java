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

import org.astraea.common.json.JsonConverter;
import org.astraea.common.json.TypeRef;
import org.junit.jupiter.api.Test;

class HttpExecutorBuilderTest {

  @Test
  void testBuilder() {
    var builder = HttpExecutor.builder();
    assertEquals(JsonConverter.defaultConverter().getClass(), builder.jsonConverter.getClass());

    builder = HttpExecutor.builder().jsonConverter(new TestJsonConverter());
    assertEquals(TestJsonConverter.class, builder.jsonConverter.getClass());
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
