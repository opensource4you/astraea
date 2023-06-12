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

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

import java.lang.reflect.Array;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

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

  public static <T> TypeRef<T> of(Class<T> clz) {
    return of((Type) clz);
  }

  public static <T> TypeRef<List<T>> array(Class<T> clz) {
    return of(new ParameterizedTypeImpl(List.class, new Type[] {clz}));
  }

  @SuppressWarnings("unchecked")
  public static TypeRef<byte[]> bytes() {
    return of((Class<byte[]>) Array.newInstance(byte.class, 0).getClass());
  }

  public static <T> TypeRef<Map<String, T>> map(Class<T> clz) {
    return of(new ParameterizedTypeImpl(Map.class, new Type[] {String.class, clz}));
  }

  public static <T> TypeRef<Set<T>> set(Class<T> clz) {
    return of(new ParameterizedTypeImpl(Set.class, new Type[] {clz}));
  }

  protected final Type type;

  protected TypeRef() {
    type = ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0];
  }

  public Type getType() {
    return type;
  }

  @Override
  public int hashCode() {
    return getType() != null ? getType().hashCode() : 0;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TypeRef<?> typeRef)) {
      return false;
    }

    return getType() != null ? getType().equals(typeRef.getType()) : typeRef.getType() == null;
  }

  private static class ParameterizedTypeImpl implements ParameterizedType {
    private final Class<?> raw;
    private final Type useOwner;
    private final Type[] typeArguments;

    ParameterizedTypeImpl(final Class<?> rawClass, final Type[] typeArguments) {
      this.raw = requireNonNull(rawClass);
      this.useOwner = rawClass.getEnclosingClass();
      this.typeArguments =
          Arrays.copyOf(requireNonNull(typeArguments), typeArguments.length, Type[].class);
    }

    @Override
    public boolean equals(final Object obj) {
      if (this == obj) return true;
      if (obj == null || getClass() != obj.getClass()) return false;
      ParameterizedTypeImpl that = (ParameterizedTypeImpl) obj;
      return Objects.equals(raw, that.raw)
          && Objects.equals(useOwner, that.useOwner)
          && Arrays.equals(typeArguments, that.typeArguments);
    }

    @Override
    public Type[] getActualTypeArguments() {
      return typeArguments.clone();
    }

    @Override
    public Type getOwnerType() {
      return useOwner;
    }

    @Override
    public Type getRawType() {
      return raw;
    }

    @Override
    public int hashCode() {
      int result = Objects.hash(raw, useOwner);
      return result * 31 + Arrays.hashCode(typeArguments);
    }

    @Override
    public String toString() {
      StringBuilder builder = new StringBuilder();
      builder.append(
          useOwner == null ? raw.getName() : format("%s.%s", useOwner, raw.getTypeName()));

      if (typeArguments.length > 0) {
        builder.append('<');
        builder.append(Arrays.stream(typeArguments).map(Type::getTypeName).collect(joining(", ")));
        builder.append('>');
      }

      return builder.toString();
    }
  }
}
