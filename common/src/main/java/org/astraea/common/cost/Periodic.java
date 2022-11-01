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
package org.astraea.common.cost;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import org.astraea.common.Utils;

/** Enables methods to be updated periodically, not calculated every time they are called. */
public abstract class Periodic<Value> {
  private long lastUpdate = -1;
  private Value value;
  /** Ensure there is an old value ready for use */
  private final CountDownLatch hasValue = new CountDownLatch(1);
  /** Guarding the expiration condition check (member variable: lastUpdate) */
  private final ReentrantLock checkLock = new ReentrantLock();
  /** Guarding the value update (member variable: value) */
  private final ReentrantLock updateLock = new ReentrantLock();

  /**
   * Updates the value interval second.
   *
   * @param updater Methods that need to be updated regularly.
   * @param interval Required interval.
   * @return an object of type Value created from the parameter value.
   */
  protected Value tryUpdate(Supplier<Value> updater, Duration interval) {
    try {
      if (checkLock.tryLock()) {
        if (Utils.isExpired(lastUpdate, interval)) {
          // it expired, call updater
          final var updatedValue = updater.get();
          updateLock.lock();
          value = updatedValue;
          lastUpdate = currentTime();
          hasValue.countDown();
          return value;
        }
      }
      Utils.packException(() -> hasValue.await());
      updateLock.lock();
      return value;
    } finally {
      if (checkLock.isHeldByCurrentThread()) checkLock.unlock();
      updateLock.unlock();
    }
  }

  /**
   * Updates the value each second.
   *
   * @param updater Methods that need to be updated regularly.
   * @return an object of type Value created from the parameter value.
   */
  protected Value tryUpdateAfterOneSecond(Supplier<Value> updater) {
    return tryUpdate(updater, Duration.ofSeconds(1));
  }

  protected long currentTime() {
    return System.currentTimeMillis();
  }
}
