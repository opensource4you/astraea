package org.astraea.consumer;

import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;

public final class Header implements org.apache.kafka.common.header.Header {

  public static Header of(String key, byte[] value) {
    return new Header(key, value);
  }

  private final String key;
  private final byte[] value;

  private Header(String key, byte[] value) {
    this.key = key;
    this.value = value;
  }

  public static Header of(org.apache.kafka.common.header.Header header) {
    return new Header(header.key(), header.value());
  }

  public static Headers of(Collection<Header> headers) {
    return new RecordHeaders(
        headers.stream()
            .map(h -> (org.apache.kafka.common.header.Header) h)
            .collect(Collectors.toList()));
  }

  public static Collection<Header> of(Headers headers) {
    return StreamSupport.stream(headers.spliterator(), false)
        .map(Header::of)
        .collect(Collectors.toList());
  }

  @Override
  public String key() {
    return key;
  }

  @Override
  public byte[] value() {
    return value;
  }
}
