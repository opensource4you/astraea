package org.astraea.web;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class WebServiceTest {

  @Test
  void testArgument() {
    var argument =
        org.astraea.argument.Argument.parse(
            new WebService.Argument(),
            new String[] {"--bootstrap.servers", "localhost", "--port", "65535"});
    Assertions.assertEquals("localhost", argument.bootstrapServers());
    Assertions.assertEquals(65535, argument.port);
  }
}
