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
package org.astraea.common.metrics.stats;

import java.util.LinkedList;
import java.util.Set;

public class SetDecreased<V> implements Stat<Set<V>, Integer> {
  private final LinkedList<Set<V>> past = new LinkedList<>();

  private final int interval;

  public SetDecreased(int interval) {
    // History sets + current set
    this.interval = interval + 1;
  }

  @Override
  public synchronized void record(Set<V> current) {
    past.add(Set.copyOf(current));

    // Remove outdated set
    while (past.size() > this.interval) {
      past.poll();
    }
  }

  @Override
  public synchronized Integer measure() {
    if (past.isEmpty()) {
      // No elements increased
      return 0;
    }

    // Not enough past record, compare with empty set.
    Set<V> last = (past.size() < this.interval) ? Set.of() : past.peekFirst();
    Set<V> current = past.peekLast();

    return (int) last.stream().filter(e -> !current.contains(e)).count();
  }
}
