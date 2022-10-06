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
package org.astraea.common;

import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;

public class LinkedHashSet<E> extends java.util.LinkedHashSet<E> {

  //    public static <E> LinkedHashSet<E> of(E e1) {
  //        return of(e1);
  //    }
  //
  //    @SuppressWarnings("unchecked")
  //    public static <E> LinkedHashSet<E> of(E e1, E e2) {
  //        return create(e1, e2);
  //    }
  //
  //    @SuppressWarnings("unchecked")
  //    public static <E> LinkedHashSet<E> of(E e1, E e2, E e3) {
  //        return create(e1, e2, e3);
  //    }
  //
  //
  //    @SuppressWarnings("unchecked")
  //    public static <E> LinkedHashSet<E> of(E e1, E e2, E e3, E e4) {
  //        return create(e1, e2, e3, e4);
  //    }
  //
  //    @SuppressWarnings("unchecked")
  //    public static <E> LinkedHashSet<E> of(E e1, E e2, E e3, E e4, E e5) {
  //        return create(e1, e2, e3, e4, e5);
  //    }
  //
  //    @SuppressWarnings("unchecked")
  //    public static <E> LinkedHashSet<E> of(E e1, E e2, E e3, E e4, E e5, E e6) {
  //        return create(e1, e2, e3, e4, e5,
  //                e6);
  //    }
  //
  //    @SuppressWarnings("unchecked")
  //    public static <E> LinkedHashSet<E> of(E e1, E e2, E e3, E e4, E e5, E e6, E e7) {
  //        return create(e1, e2, e3, e4, e5,
  //                e6, e7);
  //    }
  //
  //    @SuppressWarnings("unchecked")
  //    public static <E> LinkedHashSet<E> of(E e1, E e2, E e3, E e4, E e5, E e6, E e7, E e8) {
  //        return create(e1, e2, e3, e4, e5,
  //                e6, e7, e8);
  //    }
  //
  //    @SuppressWarnings("unchecked")
  //    public static <E> LinkedHashSet<E> of(E e1, E e2, E e3, E e4, E e5, E e6, E e7, E e8, E e9)
  // {
  //        return create(e1, e2, e3, e4, e5,
  //                e6, e7, e8, e9);
  //    }
  //
  //    @SuppressWarnings("unchecked")
  //    public static <E> LinkedHashSet<E> of(E e1, E e2, E e3, E e4, E e5, E e6, E e7, E e8, E e9,
  // E e10) {
  //        return create(e1, e2, e3, e4, e5,
  //                e6, e7, e8, e9, e10);
  //    }

  //    public static <E> LinkedHashSet<E> of(E... input) {
  //        return create(input);
  //    }

  public static <E> LinkedHashSet<E> of(E... input) {
    return new LinkedHashSet<>(Arrays.stream(input).collect(Collectors.toList()));
  }

  public LinkedHashSet(int initialCapacity, float loadFactor) {
    super(initialCapacity, loadFactor);
  }

  public LinkedHashSet(int initialCapacity) {
    super(initialCapacity);
  }

  public LinkedHashSet() {
    super();
  }

  public LinkedHashSet(Collection<? extends E> c) {
    super(c);
  }
}
