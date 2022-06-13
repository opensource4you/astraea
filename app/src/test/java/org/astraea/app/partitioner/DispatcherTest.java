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
package org.astraea.app.partitioner;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import org.astraea.app.cost.ClusterInfo;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class DispatcherTest {

  @Test
  void testNullKey() {
    var count = new AtomicInteger();
    var dispatcher =
        new Dispatcher() {
          @Override
          public int partition(String topic, byte[] key, byte[] value, ClusterInfo clusterInfo) {
            Assertions.assertEquals(0, Objects.requireNonNull(key).length);
            Assertions.assertEquals(0, Objects.requireNonNull(value).length);
            count.incrementAndGet();
            return 0;
          }

          @Override
          public void configure(Configuration config) {
            count.incrementAndGet();
          }
        };
    Assertions.assertEquals(0, count.get());
    // it should not throw NPE
    dispatcher.partition("t", null, null, null, null, null);
    Assertions.assertEquals(1, count.get());
    dispatcher.configure(Map.of("a", "b"));
    Assertions.assertEquals(2, count.get());
  }
}
