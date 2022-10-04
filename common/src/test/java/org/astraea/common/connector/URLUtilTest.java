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
package org.astraea.common.connector;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import org.junit.jupiter.api.Test;

class URLUtilTest {

  @Test
  void testGetQueryUrl() throws URISyntaxException, MalformedURLException {
    var url = new URI("http://localhost:8989/test").toURL();
    var newURL = URLUtil.getQueryUrl(url, Map.of("key1", "value1"));
    assertEquals("key1=value1", newURL.getQuery());

    newURL = URLUtil.getQueryUrl(url, Map.of("key1", "value1,value2"));
    assertEquals("key1=value1,value2", newURL.getQuery());

    newURL = URLUtil.getQueryUrl(url, Map.of("key1", "/redirectKey"));
    assertEquals("key1=%252FredirectKey", newURL.getQuery());

    newURL = URLUtil.getQueryUrl(url, Map.of("key1", "/redirectKey,/redirectKey2"));
    assertEquals("key1=%252FredirectKey,%252FredirectKey2", newURL.getQuery());
  }
}
