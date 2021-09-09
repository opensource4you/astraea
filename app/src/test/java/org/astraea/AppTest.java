package org.astraea;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.Collections;
import org.junit.jupiter.api.Test;

class AppTest {
  private static String[] ARGS;

  public static void main(String[] args) {
    ARGS = args;
  }

  @Test
  void testExecute() throws Throwable {
    App.execute(Collections.singletonList(AppTest.class), Arrays.asList("AppTest", "1", "2"));
    assertEquals(2, ARGS.length);
    assertEquals("1", ARGS[0]);
    assertEquals("2", ARGS[1]);
  }
}
