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
import org.astraea.common.Utils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class QueryTest {

  private static final LocalDateTime NOW = LocalDateTime.now();
  private static final Map<String, Object> TABLE =
      MapUtils.of(
          "port",
          111,
          "controller",
          false,
          "timestamp",
          NOW,
          "size",
          DataSize.MB.of(10),
          "nonexistent",
          "");

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

  private static void assertTrue(String queryString) {
    var query = Query.of(queryString);
    Assertions.assertNotEquals(
        0, TABLE.keySet().stream().filter(query::required).count(), "queryString: " + queryString);
    Assertions.assertTrue(query.required(TABLE));
  }

  private static void assertFalse(String queryString) {
    var query = Query.of(queryString);
    Assertions.assertNotEquals(0, TABLE.keySet().stream().filter(query::required).count());
    Assertions.assertFalse(query.required(TABLE));
  }

  @Test
  void testEmpty() {
    assertTrue("");
    assertTrue(" ");
    assertTrue(null);
  }

  @Test
  void testForEmpty() {
    var query = Query.of(Utils.randomString() + "=");
    Assertions.assertTrue(query.required(TABLE));
    Assertions.assertTrue(query.required(Map.of()));
  }

  @Test
  void testSingleQueries() {
    POSITIVE_QUERIES.forEach(QueryTest::assertTrue);
    NEGATIVE_QUERIES.forEach(QueryTest::assertFalse);
  }

  @Test
  void testMultiQueriesForAnd() {
    assertTrue(String.join("&&", POSITIVE_QUERIES));
    assertFalse(String.join("&&", NEGATIVE_QUERIES));

    // add a negative query
    assertFalse(
        Stream.concat(
                POSITIVE_QUERIES.stream(),
                Stream.of(NEGATIVE_QUERIES.get((int) (NEGATIVE_QUERIES.size() * Math.random()))))
            .collect(Collectors.joining("&&")));

    // add a positive query
    assertFalse(
        Stream.concat(
                NEGATIVE_QUERIES.stream(),
                Stream.of(POSITIVE_QUERIES.get((int) (POSITIVE_QUERIES.size() * Math.random()))))
            .collect(Collectors.joining("&&")));
  }

  @Test
  void testMultiQueriesForOr() {
    assertTrue(String.join("||", POSITIVE_QUERIES));
    assertFalse(String.join("||", NEGATIVE_QUERIES));

    // add a negative query
    assertTrue(
        Stream.concat(
                POSITIVE_QUERIES.stream(),
                Stream.of(NEGATIVE_QUERIES.get((int) (NEGATIVE_QUERIES.size() * Math.random()))))
            .collect(Collectors.joining("||")));

    // add a positive query
    assertTrue(
        Stream.concat(
                NEGATIVE_QUERIES.stream(),
                Stream.of(POSITIVE_QUERIES.get((int) (POSITIVE_QUERIES.size() * Math.random()))))
            .collect(Collectors.joining("||")));
  }
}
