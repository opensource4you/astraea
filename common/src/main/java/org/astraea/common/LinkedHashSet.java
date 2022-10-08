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

  @SuppressWarnings("unchecked")
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
