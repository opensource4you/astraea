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
import java.util.OptionalInt;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class OptionalIntTypeAdapterTest {

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  private static class Dummy {

    private Dummy(int value) {
      this.value = OptionalInt.of(value);
    }

    private Dummy() {
      this.value = OptionalInt.empty();
    }

    OptionalInt value;
  }

  @Test
  void testSerialize() {
    var gson =
        new GsonBuilder()
            .registerTypeAdapter(OptionalInt.class, new OptionalIntTypeAdapter())
            .create();
    Assertions.assertEquals("1", gson.toJson(OptionalInt.of(1)));
    Assertions.assertEquals("1024", gson.toJson(OptionalInt.of(1024)));
    Assertions.assertEquals("null", gson.toJson(OptionalInt.empty()));
    Assertions.assertEquals("{}", gson.toJson(new Dummy()));
    Assertions.assertEquals("{\"value\":5}", gson.toJson(new Dummy(5)));
  }

  @Test
  void testDeserialize() {
    var gson =
        new GsonBuilder()
            .registerTypeAdapter(OptionalInt.class, new OptionalIntTypeAdapter())
            .create();

    var json0 = "{\"value\":5}";
    var object0 = gson.fromJson(json0, Dummy.class);
    Assertions.assertEquals(5, object0.value.orElseThrow());

    var json1 = "{}";
    var object1 = gson.fromJson(json1, Dummy.class);
    Assertions.assertEquals(OptionalInt.empty(), object1.value);
  }
}
