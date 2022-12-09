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

import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * Record the latest n value. Return the difference of the current value and oldest value that was
 * recorded.
 */
public class Difference implements Stat<Double> {
  private final int length;

  // The latest N+1 value. Used to compute the difference of current value and the latest n value.
  private final ConcurrentLinkedDeque<Double> past = new ConcurrentLinkedDeque<>();

  public Difference() {
    this(1);
  }

  public Difference(int n) {
    // Last n value + current value
    this.length = n + 1;
    past.add(0.0);
  }

  @Override
  public void record(Double value) {
    past.add(value);
    while (past.size() > this.length) {
      past.poll();
    }
  }

  @Override
  public Double measure() {
    return past.getLast() - past.getFirst();
  }
}
