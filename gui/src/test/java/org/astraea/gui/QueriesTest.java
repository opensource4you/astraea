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
package org.astraea.gui;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.astraea.common.DataSize;
import org.astraea.common.MapUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class QueriesTest {

  private static final LocalDateTime NOW = LocalDateTime.now();
  private static final Map<String, Object> TABLE =
      MapUtils.of("port", 111, "controller", false, "timestamp", NOW, "size", DataSize.MB.of(10));

  private static final List<String> POSITIVE_QUERIES =
      List.of(
          "port=111",
          "port=111",
          "port>=111",
          "port<=111",
          "controller=false",
          "timestamp=" + NOW,
          "timestamp==" + NOW,
          "timestamp>=" + NOW,
          "timestamp<=" + NOW,
          "size==10MB",
          "size<=10MB",
          "size>=10MB",
          "nonexistent=");

  private static final List<String> NEGATIVE_QUERIES =
      List.of(
          "port>111",
          "port<111",
          "controller=true",
          "timestamp>" + NOW,
          "timestamp<" + NOW,
          "size=10MB",
          "size>10MB",
          "size<10MB",
          "size=");

  @Test
  void testEmpty() {
    Assertions.assertTrue(Queries.predicate("").test(TABLE));
    Assertions.assertTrue(Queries.predicate("  ").test(TABLE));
    Assertions.assertTrue(Queries.predicate(null).test(TABLE));
  }

  @Test
  void testSingleQueries() {
    POSITIVE_QUERIES.forEach(q -> Assertions.assertTrue(Queries.predicate(q).test(TABLE), q));
    NEGATIVE_QUERIES.forEach(q -> Assertions.assertFalse(Queries.predicate(q).test(TABLE), q));
  }

  @Test
  void testMultiQueriesForAnd() {
    Assertions.assertTrue(Queries.predicate(String.join("&&", POSITIVE_QUERIES)).test(TABLE));
    Assertions.assertFalse(Queries.predicate(String.join("&&", NEGATIVE_QUERIES)).test(TABLE));

    // add a negative query
    Assertions.assertFalse(
        Queries.predicate(
                Stream.concat(
                        POSITIVE_QUERIES.stream(),
                        Stream.of(
                            NEGATIVE_QUERIES.get((int) (NEGATIVE_QUERIES.size() * Math.random()))))
                    .collect(Collectors.joining("&&")))
            .test(TABLE));

    // add a positive query
    Assertions.assertFalse(
        Queries.predicate(
                Stream.concat(
                        NEGATIVE_QUERIES.stream(),
                        Stream.of(
                            POSITIVE_QUERIES.get((int) (POSITIVE_QUERIES.size() * Math.random()))))
                    .collect(Collectors.joining("&&")))
            .test(TABLE));
  }

  @Test
  void testMultiQueriesForOr() {
    Assertions.assertTrue(Queries.predicate(String.join("||", POSITIVE_QUERIES)).test(TABLE));
    Assertions.assertFalse(Queries.predicate(String.join("||", NEGATIVE_QUERIES)).test(TABLE));

    // add a negative query
    Assertions.assertTrue(
        Queries.predicate(
                Stream.concat(
                        POSITIVE_QUERIES.stream(),
                        Stream.of(
                            NEGATIVE_QUERIES.get((int) (NEGATIVE_QUERIES.size() * Math.random()))))
                    .collect(Collectors.joining("||")))
            .test(TABLE));

    // add a positive query
    Assertions.assertTrue(
        Queries.predicate(
                Stream.concat(
                        NEGATIVE_QUERIES.stream(),
                        Stream.of(
                            POSITIVE_QUERIES.get((int) (POSITIVE_QUERIES.size() * Math.random()))))
                    .collect(Collectors.joining("||")))
            .test(TABLE));
  }
}
