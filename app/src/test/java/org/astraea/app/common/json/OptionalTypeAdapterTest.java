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
package org.astraea.app.common.json;

import com.google.gson.GsonBuilder;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class OptionalTypeAdapterTest {

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  private static class Dummy {

    private <T> Dummy(T value) {
      this.value = Optional.of(value);
    }

    private Dummy() {
      this.value = Optional.empty();
    }

    Optional<?> value;
  }

  @Test
  void testSerialize() {
    var gson =
        new GsonBuilder().registerTypeAdapter(Optional.class, new OptionalTypeAdapter()).create();
    Assertions.assertEquals("1", gson.toJson(Optional.of(1)));
    Assertions.assertEquals("1024", gson.toJson(Optional.of(1024)));
    Assertions.assertEquals("\"Hello\"", gson.toJson(Optional.of("Hello")));
    Assertions.assertEquals("null", gson.toJson(Optional.empty()));
    Assertions.assertEquals("{}", gson.toJson(new Dummy()));
    Assertions.assertEquals("{\"value\":5}", gson.toJson(new Dummy(5)));
    Assertions.assertEquals("{\"value\":\"Hello\"}", gson.toJson(new Dummy("Hello")));
  }

  @Test
  void testDeserialize() {
    var gson =
        new GsonBuilder().registerTypeAdapter(Optional.class, new OptionalTypeAdapter()).create();

    var json0 = "{\"value\":\"Hello\"}";
    var object0 = gson.fromJson(json0, Dummy.class);
    Assertions.assertEquals("Hello", object0.value.orElseThrow());

    var json1 = "{\"value\":true}";
    var object1 = gson.fromJson(json1, Dummy.class);
    Assertions.assertEquals(true, object1.value.orElseThrow());

    var json2 = "{\"value\":false}";
    var object2 = gson.fromJson(json2, Dummy.class);
    Assertions.assertEquals(false, object2.value.orElseThrow());

    var json3 = "{\"value\":[\"rain\", \"drop\"]}";
    var object3 = gson.fromJson(json3, Dummy.class);
    Assertions.assertEquals(List.of("rain", "drop"), object3.value.orElseThrow());

    var json4 = "{}";
    var object4 = gson.fromJson(json4, Dummy.class);
    Assertions.assertEquals(Optional.empty(), object4.value);

    var json5 = "{\"value\":5}";
    var object5 = gson.fromJson(json5, Dummy.class);
    Assertions.assertEquals(Optional.of(5.0), object5.value);
  }
}
