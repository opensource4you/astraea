package org.astraea.web;

import com.sun.net.httpserver.HttpExchange;
import java.net.URI;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class HandlerTest {

  @Test
  void testException() {
    var exception = new IllegalStateException("hello");
    Handler handler =
        (paths, queries) -> {
          throw exception;
        };

    var httpExchange = Mockito.mock(HttpExchange.class);
    Mockito.when(httpExchange.getRequestURI()).thenReturn(URI.create("http://localhost:8888/abc"));
    var r = Assertions.assertInstanceOf(ErrorObject.class, handler.response(httpExchange));
    Assertions.assertNotEquals(200, r.code);
    Assertions.assertEquals(exception.getMessage(), r.message);
  }

  @Test
  void testParseTarget() {
    Assertions.assertFalse(
        Handler.parseTarget(URI.create("http://localhost:11111/abc")).isPresent());
    var target = Handler.parseTarget(URI.create("http://localhost:11111/abc/bbb"));
    Assertions.assertTrue(target.isPresent());
    Assertions.assertEquals("bbb", target.get());
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> Handler.parseTarget(URI.create("http://localhost:11111/abc/bbb/dddd")));
  }

  @Test
  void testParseQuery() {
    var uri = URI.create("http://localhost:11111/abc?k=v&a=b");
    var queries = Handler.parseQueries(uri);
    Assertions.assertEquals(2, queries.size());
    Assertions.assertEquals("v", queries.get("k"));
    Assertions.assertEquals("b", queries.get("a"));
  }
}
