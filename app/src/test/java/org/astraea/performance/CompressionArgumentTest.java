package org.astraea.performance;

import com.beust.jcommander.ParameterException;
import java.util.stream.Stream;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.record.CompressionType;
import org.astraea.argument.ArgumentUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class CompressionArgumentTest {

  @Test
  void testConversion() {
    var arg = new Performance.CompressionArgument();
    Stream.of(CompressionType.values())
        .forEach(type -> Assertions.assertEquals(type, arg.convert(type.name)));
    Assertions.assertThrows(ParameterException.class, () -> arg.convert("aaa"));
  }

  @Test
  void testParse() {
    var arg =
        ArgumentUtil.parseArgument(
            new Performance.Argument(),
            new String[] {"--bootstrap.servers", "aa", "--compression", "gzip"});
    Assertions.assertEquals(CompressionType.GZIP, arg.compression);
    Assertions.assertEquals(
        "gzip", arg.producerProps().get(ProducerConfig.COMPRESSION_TYPE_CONFIG));
    Assertions.assertNull(arg.props().get(ProducerConfig.COMPRESSION_TYPE_CONFIG));
  }

  @Test
  void testJmxServer() {
    var arg =
        ArgumentUtil.parseArgument(
            new Performance.Argument(),
            new String[] {"--bootstrap.servers", "aa", "--jmx.servers", "aaa"});
    Assertions.assertEquals("aaa", arg.producerProps().get("jmx_servers"));
  }
}
