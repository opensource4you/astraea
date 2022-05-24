package org.astraea.argument;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ArgumentTest {

  @Test
  void testFilterEmpty() {
    var args0 = Argument.filterEmpty(new String[] {"", " ", "   "});
    Assertions.assertEquals(0, args0.length);

    var args1 = Argument.filterEmpty(new String[] {"", "1 ", "   "});
    Assertions.assertEquals(1, args1.length);
    Assertions.assertEquals("1", args1[0]);

    var args2 = Argument.filterEmpty(new String[] {"", " 1", " 2  "});
    Assertions.assertEquals(2, args2.length);
    Assertions.assertEquals("1", args2[0]);
    Assertions.assertEquals("2", args2[1]);
  }

  @Test
  void testCommonProperties() throws IOException {
    var file = Files.createTempFile("test_basic_argument", "");
    try (var output = new BufferedWriter(new FileWriter(file.toFile()))) {
      output.write("key1=value1");
      output.newLine();
      output.write("key2=value2");
    }
    var argument =
        Argument.parse(
            new DumbArgument(),
            new String[] {
              "--bootstrap.servers", "abc", "--prop.file", file.toString(), "--configs", "a=b"
            });
    Assertions.assertEquals(1, argument.configs.size());
    Assertions.assertEquals("b", argument.configs.get("a"));
    Assertions.assertEquals(file.toString(), argument.propFile);
    // 2 (from prop file) + 1 (from configs) + 1 (bootstrap servers)
    Assertions.assertEquals(4, argument.configs().size());
    Assertions.assertEquals("abc", argument.bootstrapServers);
    Assertions.assertEquals("abc", argument.bootstrapServers());
    Assertions.assertEquals("value1", argument.configs().get("key1"));
    Assertions.assertEquals("value2", argument.configs().get("key2"));
  }

  private static class DumbArgument extends Argument {}
}
