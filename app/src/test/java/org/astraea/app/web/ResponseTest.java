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
  void test404() {
    var error = Assertions.assertInstanceOf(Response.ResponseImpl.class, Response.for404("this"));
    Assertions.assertEquals(404, error.code);
    Assertions.assertEquals("this", error.message);
  }

  @Test
  void testOk() {
    var response = Response.ok();
    Assertions.assertEquals(0, response.json().length());
    Assertions.assertEquals(0, response.json().getBytes(StandardCharsets.UTF_8).length);
  }

  @Test
  void testAccept() {
    var response = Response.accept();
    Assertions.assertEquals(0, response.json().length());
    Assertions.assertEquals(0, response.json().getBytes(StandardCharsets.UTF_8).length);
  }
}
