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
import java.util.Objects;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;

public interface HdfsServer extends AutoCloseable {

  static HdfsServer local() throws IOException {
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
      conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, homeFolder.getAbsolutePath());
      miniDfsBuilder = new MiniDFSCluster.Builder(conf);
    }

    private File homeFolder = Utils.createTempDirectory("local_hdfs");

    private MiniDFSCluster.Builder miniDfsBuilder;

    public void checkArguments() {
      if (!homeFolder.exists() && !homeFolder.mkdir())
        throw new IllegalArgumentException(
            "fail to create folder on " + homeFolder.getAbsolutePath());
      if (!homeFolder.isDirectory())
        throw new IllegalArgumentException(homeFolder.getAbsolutePath() + " is not folder");
    }

    public Builder homeFolder(File homeFolder) {
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
          return "root";
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
