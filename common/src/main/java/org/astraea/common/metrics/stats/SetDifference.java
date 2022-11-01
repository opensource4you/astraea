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

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;
import org.astraea.common.EnumInfo;

public class SetDifference<V>
    implements Stat<SetDifference.SetOperation<V>, SetDifference.Result<V>> {

  private final Set<V> current = new HashSet<>();
  private final ConcurrentLinkedQueue<Set<V>> past = new ConcurrentLinkedQueue<>();

  private final int interval;

  public SetDifference(int interval) {
    this.interval = interval;
  }

  @Override
  public void record(SetOperation<V> value) {
    if (value.operation() == SetOperation.Operation.ADD) {
      past.add(Set.copyOf(current));
      current.addAll(value.set());
    } else if (value.operation() == SetOperation.Operation.REMOVE) {
      past.add(Set.copyOf(current));
      current.removeAll(value.set());
    }

    // Remove outdated set
    while (past.size() > this.interval) {
      past.poll();
    }
  }

  @Override
  public Result<V> measure() {
    var current = Collections.unmodifiableSet(this.current);

    return new Result<>() {
      @Override
      public Set<V> current() {
        return current;
      }

      @Override
      public Set<V> increased() {
        var pastSet = past.peek();
        if (pastSet == null) return Set.of();
        return current.stream()
            .filter(e -> !pastSet.contains(e))
            .collect(Collectors.toUnmodifiableSet());
      }

      @Override
      public Set<V> removed() {
        var pastSet = past.peek();
        if (pastSet == null) return Set.of();
        return pastSet.stream()
            .filter(e -> !current.contains(e))
            .collect(Collectors.toUnmodifiableSet());
      }

      @Override
      public Set<V> unchanged() {
        var pastSet = past.peek();
        if (pastSet == null) return Set.of();
        return pastSet.stream().filter(current::contains).collect(Collectors.toUnmodifiableSet());
      }
    };
  }

  public static class SetOperation<V> {

    private final Operation operation;
    private final Set<V> set;

    SetOperation(Operation operation, Set<V> set) {
      this.operation = operation;
      this.set = Collections.unmodifiableSet(set);
    }

    public enum Operation implements EnumInfo {
      ADD("add"),
      REMOVE("remove");

      private final String name;

      Operation(String name) {
        this.name = name;
      }

      public static Operation ofAlias(String name) {
        return EnumInfo.ignoreCaseEnum(Operation.class, name);
      }

      @Override
      public String alias() {
        return this.name;
      }

      @Override
      public String toString() {
        return this.name;
      }
    }

    public static <V> SetOperation<V> ofAdd(Set<V> added) {
      return new SetOperation<>(Operation.ADD, added);
    }

    public static <V> SetOperation<V> ofRemove(Set<V> removed) {
      return new SetOperation<>(Operation.REMOVE, removed);
    }

    public Set<V> set() {
      return this.set;
    }

    public Operation operation() {
      return this.operation;
    }
  }

  public interface Result<V> {
    Set<V> current();

    Set<V> increased();

    Set<V> removed();

    Set<V> unchanged();
  }
}
