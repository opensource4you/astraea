/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.astraea.common.backup;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.StandardCharsets;

public final class ByteBufferUtils {

  public static int readInt(ReadableByteChannel channel) {
    var buf = ByteBuffer.allocate(Integer.BYTES);
    try {
      var size = channel.read(buf);
      if (size != Integer.BYTES)
        throw new IllegalStateException(
            "The remaining size is " + size + ", but expected is " + Integer.BYTES);
      return buf.flip().getInt();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public static int readInt(InputStream fs) {
    var buf = ByteBuffer.allocate(Integer.BYTES).array();
    try {
      var size = fs.read(buf);
      if (size != Integer.BYTES)
        throw new IllegalStateException(
            "The remaining size is " + size + ", but expected is " + Integer.BYTES);
      return ByteBuffer.wrap(buf).getInt();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public static short readShort(ReadableByteChannel channel) {
    var buf = ByteBuffer.allocate(Short.BYTES);
    try {
      var size = channel.read(buf);
      if (size != Short.BYTES)
        throw new IllegalStateException(
            "The remaining size is " + size + ", but expected is " + Short.BYTES);
      return buf.flip().getShort();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public static short readShort(InputStream fs) {
    var buf = ByteBuffer.allocate(Short.BYTES).array();
    try {
      var size = fs.read(buf);
      if (size != Short.BYTES)
        throw new IllegalStateException(
            "The remaining size is " + size + ", but expected is " + Short.BYTES);
      return ByteBuffer.wrap(buf).getShort();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public static String readString(ByteBuffer buffer, int size) {
    if (size < 0) return null;
    var dst = new byte[size];
    buffer.get(dst);
    return new String(dst, StandardCharsets.UTF_8);
  }

  /**
   * @return null if the size is smaller than zero
   */
  public static byte[] readBytes(ByteBuffer buffer, int size) {
    if (size < 0) return null;
    var dst = new byte[size];
    buffer.get(dst);
    return dst;
  }

  public static ByteBuffer of(short value) {
    var buf = ByteBuffer.allocate(Short.BYTES);
    buf.putShort(value);
    return buf.flip();
  }

  public static ByteBuffer of(int value) {
    var buf = ByteBuffer.allocate(Integer.BYTES);
    buf.putInt(value);
    return buf.flip();
  }

  public static void putLengthBytes(ByteBuffer buffer, byte[] value) {
    if (value == null) buffer.putInt(-1);
    else {
      buffer.putInt(value.length);
      buffer.put(ByteBuffer.wrap(value));
    }
  }

  public static void putLengthString(ByteBuffer buffer, String value) {
    if (value == null) buffer.putShort((short) -1);
    else {
      var valueByte = value.getBytes(StandardCharsets.UTF_8);
      buffer.putShort((short) valueByte.length);
      buffer.put(ByteBuffer.wrap(valueByte));
    }
  }
}
