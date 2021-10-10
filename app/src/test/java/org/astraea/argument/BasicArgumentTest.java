package org.astraea.argument;

import java.io.*;
import java.nio.file.Files;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class BasicArgumentTest {

  @Test
  void testCommonProperties() throws IOException {
    var file = Files.createTempFile("test_basic_argument", "");
    try (var output = new BufferedWriter(new FileWriter(file.toFile()))) {
      output.write("key1=value1");
      output.newLine();
      output.write("key2=value2");
    }
    var argument =
        ArgumentUtil.parseArgument(
            new DumbArgument(),
            new String[] {"--bootstrap.servers", "abc", "--props.file", file.toString()});
    Assertions.assertEquals(3, argument.properties().size());
    Assertions.assertEquals("abc", argument.brokers);
    Assertions.assertEquals("value1", argument.properties().get("key1").toString());
    Assertions.assertEquals("value2", argument.properties().get("key2").toString());
  }

  private static class DumbArgument extends BasicArgument {}
}
