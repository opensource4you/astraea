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
package org.astraea.common.metrics.collector;

import java.util.List;
import java.util.Optional;
import org.astraea.common.metrics.HasBeanObject;
import org.astraea.common.metrics.MBeanClient;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class FetcherTest {

  @Test
  void testMultipleFetchers() {
    var mbean0 = Mockito.mock(HasBeanObject.class);
    Fetcher fetcher0 = client -> List.of(mbean0);
    var mbean1 = Mockito.mock(HasBeanObject.class);
    Fetcher fetcher1 = client -> List.of(mbean1);

    var fetcher = Fetcher.of(List.of(fetcher0, fetcher1), e -> {}).get();

    var result = fetcher.fetch(Mockito.mock(MBeanClient.class));

    Assertions.assertEquals(2, result.size());
    Assertions.assertTrue(result.contains(mbean0));
    Assertions.assertTrue(result.contains(mbean1));
  }

  @Test
  void testEmpty() {
    Assertions.assertEquals(Optional.empty(), Fetcher.of(List.of(), e -> {}));
  }

  @Test
  void testSwallowException() {
    var result = List.of(Mockito.mock(HasBeanObject.class));
    Fetcher goodFetcher = client -> result;
    Fetcher badFetcher =
        client -> {
          throw new RuntimeException("xxx");
        };
    var fetcher = Fetcher.of(List.of(badFetcher, goodFetcher), e -> {}).get();
    Assertions.assertEquals(result, fetcher.fetch(Mockito.mock(MBeanClient.class)));
  }
}
