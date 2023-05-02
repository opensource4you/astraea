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
package org.astraea.common.metrics;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.astraea.common.admin.ClusterInfo;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class MetricSeriesBuilderTest {

  private static final ClusterInfo cluster =
      ClusterInfo.builder()
          .addNode(Set.of(1, 2, 3))
          .addFolders(
              Map.ofEntries(
                  Map.entry(1, Set.of("/folder0", "/folder1", "/folder2")),
                  Map.entry(2, Set.of("/folder0", "/folder1", "/folder2")),
                  Map.entry(3, Set.of("/folder0", "/folder1", "/folder2"))))
          .addTopic("topicA", 100, (short) 2)
          .build();

  @Test
  void example() {
    {
      // sample 10 second, 2 second interval
      var beans =
          MetricSeriesBuilder.builder()
              .cluster(cluster)
              .timeRange(LocalDateTime.now(), Duration.ofSeconds(10))
              .sampleInterval(Duration.ofSeconds(2))
              .seriesByBroker((Gen, broker) -> Stream.of(() -> null))
              .build();
      Assertions.assertEquals(Set.of(1, 2, 3), beans.all().keySet());
      Assertions.assertEquals(6, beans.all().get(1).size());
      Assertions.assertEquals(6, beans.all().get(2).size());
      Assertions.assertEquals(6, beans.all().get(3).size());
    }
    {
      // sample 10 second, 4 second interval
      var beans =
          MetricSeriesBuilder.builder()
              .cluster(cluster)
              .timeRange(LocalDateTime.now(), Duration.ofSeconds(10))
              .sampleInterval(Duration.ofSeconds(4))
              .seriesByBroker((Gen, broker) -> Stream.of(() -> null))
              .build();
      Assertions.assertEquals(Set.of(1, 2, 3), beans.all().keySet());
      Assertions.assertEquals(3, beans.all().get(1).size());
      Assertions.assertEquals(3, beans.all().get(2).size());
      Assertions.assertEquals(3, beans.all().get(3).size());
    }
    {
      // zero duration, sample just once
      var beans =
          MetricSeriesBuilder.builder()
              .cluster(cluster)
              .timeRange(LocalDateTime.now(), Duration.ZERO)
              .seriesByBroker((Gen, broker) -> Stream.of(() -> null))
              .build();
      Assertions.assertEquals(Set.of(1, 2, 3), beans.all().keySet());
      Assertions.assertEquals(1, beans.all().get(1).size());
      Assertions.assertEquals(1, beans.all().get(2).size());
      Assertions.assertEquals(1, beans.all().get(3).size());
    }
  }

  @Test
  @DisplayName(
      "By change the setting between series calls, we can have difference sample rate for each series")
  void testFlexibility() {
    var beans =
        MetricSeriesBuilder.builder()
            .cluster(cluster)
            .timeRange(LocalDateTime.now(), Duration.ofSeconds(10))
            .sampleInterval(Duration.ofSeconds(1))
            .seriesByBroker((Gen, broker) -> broker == 1 ? Stream.of(() -> null) : Stream.of())
            .timeRange(LocalDateTime.now(), Duration.ofSeconds(10))
            .sampleInterval(Duration.ofSeconds(2))
            .seriesByBroker((Gen, broker) -> broker == 2 ? Stream.of(() -> null) : Stream.of())
            .timeRange(LocalDateTime.now(), Duration.ofSeconds(15))
            .sampleInterval(Duration.ofSeconds(5))
            .seriesByBroker((Gen, broker) -> broker == 3 ? Stream.of(() -> null) : Stream.of())
            .build();
    Assertions.assertEquals(Set.of(1, 2, 3), beans.all().keySet());
    Assertions.assertEquals(11, beans.all().get(1).size());
    Assertions.assertEquals(6, beans.all().get(2).size());
    Assertions.assertEquals(4, beans.all().get(3).size());
  }

  @Test
  void testInterval() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> MetricSeriesBuilder.builder().sampleInterval(Duration.ZERO));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> MetricSeriesBuilder.builder().sampleInterval(Duration.ofSeconds(-1)));
  }
}
