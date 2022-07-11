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
package org.astraea.app.balancer.log;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.astraea.app.admin.TopicPartition;
import org.junit.jupiter.api.Test;

public class ClusterLogAllocationPerformanceTest {

  @Test
  void test() {
    var count = 10000;
    var allocation =
        ClusterLogAllocation.of(
            IntStream.range(0, count)
                .boxed()
                .collect(
                    Collectors.toMap(
                        i -> new TopicPartition("topic", i),
                        i -> List.of(LogPlacement.of(i, "/tmp/data-" + i)))));

    var start = System.currentTimeMillis();
    var current = allocation;
    for (var i = 0; i != count; ++i) {
      current = current.migrateReplica(new TopicPartition("topic", i), i, i + 1);
    }

    var elapsed = System.currentTimeMillis() - start;
    System.out.println("[CHIA] elapsed: " + elapsed);
  }
}
