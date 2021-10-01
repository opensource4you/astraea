package org.astraea.metrics.kafka;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.OutputStream;
import java.io.PrintStream;
import java.net.MalformedURLException;

import static org.junit.jupiter.api.Assertions.*;

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
        assertThrows(IllegalArgumentException.class, () -> KafkaMetricClientApp.main(new String[1]));
    }

}