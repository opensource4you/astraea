package org.astraea.app.web;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ErrorObjectTest {

  @Test
  void test404() {
    var error = ErrorObject.for404("this");
    Assertions.assertEquals(404, error.code);
    Assertions.assertEquals("this", error.message);
  }
}
