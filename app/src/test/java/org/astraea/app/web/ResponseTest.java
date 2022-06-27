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
package org.astraea.app.web;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ResponseTest {

  @Test
  void testNotFound() {
    Assertions.assertEquals(0, Response.NOT_FOUND.json().length());
    Assertions.assertEquals(404, Response.NOT_FOUND.code());
    Assertions.assertEquals(0, Response.NOT_FOUND.json().getBytes(StandardCharsets.UTF_8).length);
  }

  @Test
  void testOk() {
    Assertions.assertEquals(0, Response.OK.json().length());
    Assertions.assertEquals(200, Response.OK.code());
    Assertions.assertEquals(0, Response.OK.json().getBytes(StandardCharsets.UTF_8).length);
  }

  @Test
  void testAccept() {
    Assertions.assertEquals(0, Response.ACCEPT.json().length());
    Assertions.assertEquals(202, Response.ACCEPT.code());
    Assertions.assertEquals(0, Response.ACCEPT.json().getBytes(StandardCharsets.UTF_8).length);
  }
}
