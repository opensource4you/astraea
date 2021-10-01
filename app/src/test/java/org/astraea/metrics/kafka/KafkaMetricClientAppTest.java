package org.astraea.metrics.kafka;

import static org.junit.jupiter.api.Assertions.*;

import java.io.OutputStream;
import java.io.PrintStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class KafkaMetricClientAppTest {

  private final PrintStream stderr = System.err;

  @BeforeEach
  void beforeEach() {
    System.setErr(new PrintStream(OutputStream.nullOutputStream()));
  }

  @AfterEach
  void afterEach() {
    System.setErr(stderr);
  }

  @Test
  void testBadArguments() {
    assertThrows(IllegalArgumentException.class, () -> KafkaMetricClientApp.main(new String[0]));
  }
}
