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

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

/** ParentType didn't erase , use reflection to get that type */
public abstract class TypeRef<T> {

  public static <T> TypeRef<T> of(Type _type) {
    return new TypeRef<>() {
      @Override
      public Type getType() {
        return _type;
      }
    };
  }

  protected final Type type;

  protected TypeRef() {
    type = ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0];
  }

  public Type getType() {
    return type;
  }
}
