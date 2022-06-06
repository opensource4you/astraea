package org.astraea.app.web;

import com.sun.net.httpserver.HttpExchange;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class PostRequestTest {

  @Test
  void testParseHttpExchange() throws IOException {
    var input =
        new ByteArrayInputStream("{\"a\":\"b\",\"c\":123}".getBytes(StandardCharsets.UTF_8));
    var exchange = Mockito.mock(HttpExchange.class);
    Mockito.when(exchange.getRequestBody()).thenReturn(input);

    var request = PostRequest.of(exchange);
    Assertions.assertEquals(2, request.raw().size());
    Assertions.assertEquals("b", request.raw().get("a"));
    Assertions.assertEquals(123, request.intValue("c"));
  }

  @Test
  void testHandleDouble() {
    Assertions.assertEquals("10", PostRequest.handleDouble(10.00));
    Assertions.assertEquals("10.01", PostRequest.handleDouble(10.01));
    Assertions.assertEquals("xxxx", PostRequest.handleDouble("xxxx"));
  }

  @Test
  void testParseJson() {
    var request = PostRequest.of("{\"a\":1234, \"b\":3.34}");
    Assertions.assertEquals(1234, request.intValue("a"));
    Assertions.assertEquals(1234, request.shortValue("a"));
    Assertions.assertEquals(1234.0, request.shortValue("a"));
    Assertions.assertEquals(3.34, request.doubleValue("b"));
  }
}
