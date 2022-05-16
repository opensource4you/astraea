package org.astraea.web;

import com.sun.net.httpserver.HttpExchange;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class PostRequestTest {

  @Test
  void testParse() throws IOException {
    var input =
        new ByteArrayInputStream("{\"a\":\"b\",\"c\":123}".getBytes(StandardCharsets.UTF_8));
    var exchange = Mockito.mock(HttpExchange.class);
    Mockito.when(exchange.getRequestBody()).thenReturn(input);

    var request = PostRequest.of(exchange);
    Assertions.assertEquals(2, request.raw().size());
    Assertions.assertEquals("b", request.raw().get("a"));
    Assertions.assertEquals(123, request.intValue("c"));
  }
}
