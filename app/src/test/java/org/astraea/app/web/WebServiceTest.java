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

import java.util.concurrent.ThreadLocalRandom;
import org.astraea.app.argument.Argument;
import org.astraea.common.admin.Admin;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.Mockito;

public class WebServiceTest {

  @Test
  void testArgument() {
    var argument =
        Argument.parse(
            new WebService.Argument(),
            new String[] {"--bootstrap.servers", "localhost", "--port", "65535"});
    Assertions.assertEquals("localhost", argument.bootstrapServers());
    Assertions.assertEquals(65535, argument.port);
  }

  @Timeout(10)
  @Test
  void testClose() {
    var web = new WebService(Mockito.mock(Admin.class), 0, id -> -1);
    web.close();
  }

  @Test
  void testJmxPort() {
    var defaultPort = ThreadLocalRandom.current().nextInt(1, 65535);
    var portA = ThreadLocalRandom.current().nextInt(1, 65535);
    var portB = ThreadLocalRandom.current().nextInt(1, 65535);
    var portC = ThreadLocalRandom.current().nextInt(1, 65535);
    var argument =
        Argument.parse(
            new WebService.Argument(),
            new String[] {
              "--bootstrap.servers",
              "localhost",
              "--port",
              "65535",
              "--jmx.port",
              Integer.toString(defaultPort),
              "--jmx.ports",
              String.format("1=%d,2=%d,3=%d", portA, portB, portC)
            });
    Assertions.assertEquals(portA, argument.jmxPortMapping(1));
    Assertions.assertEquals(portB, argument.jmxPortMapping(2));
    Assertions.assertEquals(portC, argument.jmxPortMapping(3));
    Assertions.assertEquals(defaultPort, argument.jmxPortMapping(4));
    Assertions.assertEquals(defaultPort, argument.jmxPortMapping(5));
    Assertions.assertEquals(defaultPort, argument.jmxPortMapping(6));

    var noDefaultArgument =
        Argument.parse(
            new WebService.Argument(),
            new String[] {
              "--bootstrap.servers", "localhost",
              "--port", "65535",
              "--jmx.ports", String.format("1=%d,2=%d,3=%d", portA, portB, portC)
            });
    Assertions.assertEquals(portA, noDefaultArgument.jmxPortMapping(1));
    Assertions.assertEquals(portB, noDefaultArgument.jmxPortMapping(2));
    Assertions.assertEquals(portC, noDefaultArgument.jmxPortMapping(3));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> noDefaultArgument.jmxPortMapping(4));
  }
}
