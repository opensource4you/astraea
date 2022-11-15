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
package org.astraea.it;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.util.stream.Stream;

public final class Utils {

  public static int availablePort() {
    try (ServerSocket socket = new ServerSocket(0)) {
      socket.setReuseAddress(true);
      var port = socket.getLocalPort();
      // the port smaller than 1024 may be protected by OS.
      return port > 1024 ? port : port + 1024;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static int resolvePort(int port) {
    if (port <= 0) return availablePort();
    return port;
  }

  public static File createTempDirectory(String prefix) {
    try {
      return Files.createTempDirectory(prefix).toFile();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static String hostname() {
    try {
      return InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      throw new RuntimeException(e);
    }
  }

  public static void delete(File file) {
    try {
      if (file.isDirectory()) {
        var fs = file.listFiles();
        if (fs != null) Stream.of(fs).forEach(Utils::delete);
      }
      Files.deleteIfExists(file.toPath());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static File mkdir(String address) {
    try {
      return Files.createDirectories(new File(address).toPath()).toFile();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private Utils() {}
}
