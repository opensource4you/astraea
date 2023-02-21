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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;

public interface HdfsServer extends AutoCloseable {

  static HdfsServer local() {
    return builder().build();
  }

  static Builder builder() {
    return new Builder();
  }

  String hostname();

  int port();

  String user();

  @Override
  void close();

  class Builder {

    private Builder() {
      Configuration conf = new Configuration();
      conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, homeFolder.toAbsolutePath().toString());
      miniDfsBuilder = new MiniDFSCluster.Builder(conf);
    }

    private Path homeFolder = Utils.createTempDirectory("local_hdfs");

    private MiniDFSCluster.Builder miniDfsBuilder;

    private void checkArguments() {
      if (Files.notExists(homeFolder))
        try {
          Files.createDirectories(homeFolder);
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      if (!Files.isDirectory(homeFolder))
        throw new IllegalArgumentException(homeFolder + " is not folder");
    }

    public Builder homeFolder(Path homeFolder) {
      this.homeFolder = Objects.requireNonNull(homeFolder);
      return this;
    }

    public Builder controlPort(int port) {
      this.miniDfsBuilder = this.miniDfsBuilder.nameNodePort(port);
      return this;
    }

    public HdfsServer build() {
      checkArguments();

      MiniDFSCluster hdfsCluster;

      try {
        hdfsCluster =
            this.miniDfsBuilder
                .manageDataDfsDirs(true)
                .manageDataDfsDirs(true)
                .format(true)
                .build();
        hdfsCluster.waitClusterUp();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

      return new HdfsServer() {
        @Override
        public String hostname() {
          return hdfsCluster.getNameNode().getHostAndPort().split(":")[0];
        }

        @Override
        public int port() {
          return hdfsCluster.getNameNodePort();
        }

        @Override
        public String user() {
          return System.getProperty("user.name");
        }

        @Override
        public void close() {
          hdfsCluster.close();
          Utils.delete(homeFolder);
        }
      };
    }
  }
}
