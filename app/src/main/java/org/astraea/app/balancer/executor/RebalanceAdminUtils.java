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
package org.astraea.app.balancer.executor;

import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

class RebalanceAdminUtils {

  static CompletableFuture<Void> submitProgressCheck(
      Duration delay, int debounceCount, Callable<Boolean> check) {
    final var completableFuture = new CompletableFuture<Void>();
    final var nextDelay = (Function<Duration, Duration>) (current) -> delay;
    submitProgressCheck(completableFuture, check, debounceCount, 0, Duration.ZERO, nextDelay);
    return completableFuture;
  }

  private static void submitProgressCheck(
      CompletableFuture<Void> completableFuture,
      Callable<Boolean> check,
      int debounceCount,
      int acc,
      Duration delay,
      Function<Duration, Duration> nextDelay) {
    CompletableFuture.delayedExecutor(delay.toMillis(), TimeUnit.MILLISECONDS)
        .execute(
            () -> {
              try {
                // is the future already done?
                if (completableFuture.isCancelled()) return;
                if (completableFuture.isCompletedExceptionally()) return;
                if (completableFuture.isDone()) return;
                // perform check
                boolean fulfilled = check.call();
                if (fulfilled && (acc + 1) >= debounceCount) {
                  // it is done
                  completableFuture.complete(null);
                } else if (fulfilled) {
                  // condition fulfilled, but we have to debounce a few times to ensure it is stable
                  submitProgressCheck(
                      completableFuture,
                      check,
                      debounceCount,
                      acc + 1,
                      nextDelay.apply(delay),
                      nextDelay);
                } else {
                  // the condition is not fulfilled yet, submit another progress check
                  submitProgressCheck(
                      completableFuture,
                      check,
                      debounceCount,
                      0,
                      nextDelay.apply(delay),
                      nextDelay);
                }
              } catch (Exception e) {
                if (!completableFuture.isDone()) completableFuture.completeExceptionally(e);
              }
            });
  }
}
