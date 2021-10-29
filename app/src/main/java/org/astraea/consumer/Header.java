package org.astraea.consumer;

import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;

public interface Header extends org.apache.kafka.common.header.Header {

  static Header of(String key, byte[] value) {
    return new Header() {
      @Override
      public String key() {
        return key;
      }

      @Override
      public byte[] value() {
        return value;
      }
    };
  }

  static Header of(org.apache.kafka.common.header.Header header) {
    return of(header.key(), header.value());
  }

  static Headers of(Collection<Header> headers) {
    return new RecordHeaders(
        headers.stream()
            .map(h -> (org.apache.kafka.common.header.Header) h)
            .collect(Collectors.toList()));
  }

  static Collection<Header> of(Headers headers) {
    return StreamSupport.stream(headers.spliterator(), false)
        .map(Header::of)
        .collect(Collectors.toList());
  }
}
