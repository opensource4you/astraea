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
package org.astraea.app.concurrent;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import org.astraea.app.common.Utils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

public class ThreadPoolTest {

  private static class CountExecutor implements Executor {

    private final AtomicInteger executeCount = new AtomicInteger();
    private final AtomicInteger closeCount = new AtomicInteger();
    private final AtomicInteger wakeupCount = new AtomicInteger();

    @Override
    public State execute() {
      executeCount.incrementAndGet();
      return State.RUNNING;
    }

    @Override
    public void close() {
      closeCount.incrementAndGet();
    }

    @Override
    public void wakeup() {
      wakeupCount.incrementAndGet();
    }
  }

  @Test
  void testSubmitThread() throws Exception {
    var executor = new CountExecutor();
    try (var pool = ThreadPool.builder().executor(executor).build()) {
      Utils.sleep(Duration.ofSeconds(2));
    }
    Assertions.assertTrue(executor.executeCount.get() > 0);
    Assertions.assertEquals(1, executor.closeCount.get());
    Assertions.assertEquals(1, executor.wakeupCount.get());
  }

  @Test
  void testWaitAll() {
    try (var pool = ThreadPool.builder().executor(() -> State.DONE).build()) {
      pool.waitAll();
    }
  }

  @Timeout(10)
  @Test
  void testInterrupt() {
    var pool =
        ThreadPool.builder()
            .executor(
                () -> {
                  Utils.sleep(Duration.ofSeconds(100));
                  return State.DONE;
                })
            .build();
    pool.close();
    Assertions.assertTrue(pool.isClosed());
  }

  @Test
  void testEmpty() {
    try (var empty = ThreadPool.builder().build()) {
      Assertions.assertEquals(ThreadPool.EMPTY, empty);
      Assertions.assertEquals(0, empty.size());
      Assertions.assertTrue(empty.isClosed());
    }
  }
}
