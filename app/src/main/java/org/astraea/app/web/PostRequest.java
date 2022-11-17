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

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import org.astraea.common.Utils;
import org.astraea.common.json.JsonConverter;
import org.astraea.common.json.TypeRef;

public interface PostRequest {

  PostRequest EMPTY = PostRequest.of("{}");

  static PostRequest of(Map<String, ?> map) {
    return of(JsonConverter.defaultConverter().toJson(map));
  }

  static PostRequest of(String json) {
    return new PostRequest() {
      @Override
      public <T> T getRequest(TypeRef<T> typeRef) {
        var t = JsonConverter.defaultConverter().fromJson(json, typeRef);
        preventNull("$", t);
        return t;
      }
    };
  }

  /**
   * User Optional to handle null value.
   *
   * @param prefix use prefix to locate the error key
   */
  private static void preventNull(String prefix, Object obj) {
    if (obj == null)
      throw new IllegalArgumentException(String.format("Value `%s` is required.", prefix));

    var objClass = obj.getClass();

    if (Collection.class.isAssignableFrom(objClass)) {
      var list = (Collection<?>) obj;
      list.forEach(value -> preventNull(prefix + "[]", value));
    } else if (Optional.class == objClass) {
      var opt = (Optional<?>) obj;
      opt.ifPresent(o -> preventNull(prefix, o));
    } else if (Map.class.isAssignableFrom(objClass)) {
      var map = (Map<?, ?>) obj;
      map.values().forEach(x -> preventNull(prefix + "{}", x));
    } else if (Utils.isPojo(objClass)) {
      var declaredFields = objClass.getDeclaredFields();
      Arrays.stream(declaredFields)
          .forEach(
              x -> {
                x.setAccessible(true);
                preventNull(prefix + "." + x.getName(), Utils.packException(() -> x.get(obj)));
              });
    }
  }

  /** Convert object to the type T. */
  static <T> T convert(Object obj, TypeRef<T> typeRef) {
    var converter = JsonConverter.defaultConverter();
    var t = converter.fromJson(converter.toJson(obj), typeRef);
    preventNull("$", t);
    return t;
  }

  <T> T getRequest(TypeRef<T> typeRef);
}
