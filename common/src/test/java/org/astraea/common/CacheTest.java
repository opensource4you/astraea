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
package org.astraea.common;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class CacheTest {

  @Test
  void testGetAndRequire() {
    var cache = Cache.<String, String>builder().expireAfterAccess(Duration.ofSeconds(1)).build();
    Assertions.assertEquals(Optional.empty(), cache.get("xxx"));
    Assertions.assertThrows(NullPointerException.class, () -> cache.require("xxx"));
    Assertions.assertEquals("aaa", cache.require("xxx", k -> "aaa"));
  }

  @Test
  void testCacheCleanup() {
    var cache =
        Cache.<String, String>builder(key -> key + "-test")
            .expireAfterAccess(Duration.ofSeconds(1))
            .build();
    cache.require("foo");
    // this won't do anything since expire time is 1 second
    cache.cleanup();
    cache.require("bar");
    // this won't do anything since expire time is 1 second
    cache.cleanup();
    // this won't do anything since expire time is 1 second
    cache.cleanup();

    Utils.sleep(Duration.ofSeconds(1));
    Assertions.assertEquals(2, cache.size());
    // after 1 second, every element in cache has expired
    cache.cleanup();
    Assertions.assertEquals(0, cache.size());
  }

  @Test
  void testCache() {
    var onRemoveList = new ArrayList<String>();
    var cache =
        Cache.<String, String>builder(key -> key + "-test")
            .expireAfterAccess(Duration.ofSeconds(1))
            .maxCapacity(2)
            .removalListener((k, v) -> onRemoveList.add(k))
            .build();

    Assertions.assertEquals("foo-test", cache.require("foo"));
    Assertions.assertEquals("bar-test", cache.require("bar"));
    // this won't throw error since foo is cached
    Assertions.assertEquals("foo-test", cache.require("foo"));
    Assertions.assertThrowsExactly(RuntimeException.class, () -> cache.require("error"));
    Utils.sleep(Duration.ofSeconds(1));
    // trigger remove listener after 1s
    cache.require("");

    var expected = List.of("foo", "bar");
    Assertions.assertTrue(
        onRemoveList.size() == expected.size()
            && expected.containsAll(onRemoveList)
            && onRemoveList.containsAll(expected));
  }
}
