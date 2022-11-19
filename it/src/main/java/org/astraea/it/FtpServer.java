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
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ftpserver.ConnectionConfigFactory;
import org.apache.ftpserver.DataConnectionConfigurationFactory;
import org.apache.ftpserver.FtpServerFactory;
import org.apache.ftpserver.listener.ListenerFactory;
import org.apache.ftpserver.usermanager.PropertiesUserManagerFactory;
import org.apache.ftpserver.usermanager.impl.BaseUser;
import org.apache.ftpserver.usermanager.impl.WritePermission;

public interface FtpServer extends AutoCloseable {

  /**
   * create a ftp server on local. It use a random port for handling control command, and three
   * random ports are used to send/receive data to/from client.
   *
   * @return ftp server
   */
  static FtpServer local() {
    return builder().dataPorts(Arrays.asList(0, 0, 0)).build();
  }

  static Builder builder() {
    return new Builder();
  }

  String hostname();

  int port();

  String user();

  String password();

  /**
   * If the ftp server is in passive mode, the port is used to transfer data
   *
   * @return data port
   */
  List<Integer> dataPorts();

  String absolutePath();

  @Override
  void close();

  class Builder {
    private Builder() {}

    private File homeFolder = Utils.createTempDirectory("local_ftp");
    private String user = "user";
    private String password = "password";
    private int controlPort = 0;
    private List<Integer> dataPorts = Arrays.asList(0, 0, 0);

    public Builder homeFolder(File homeFolder) {
      this.homeFolder = Objects.requireNonNull(homeFolder);
      return this;
    }

    public Builder user(String user) {
      this.user = user;
      return this;
    }

    public Builder password(String password) {
      this.password = password;
      return this;
    }

    public Builder controlPort(int controlPort) {
      this.controlPort = controlPort;
      return this;
    }

    /**
     * set the ports used to translate data. NOTED: the max connection of data is equal to number of
     * data ports.
     *
     * @param dataPorts data ports
     * @return this builder
     */
    public Builder dataPorts(List<Integer> dataPorts) {
      this.dataPorts = dataPorts;
      return this;
    }

    private void checkArguments() {
      if (!homeFolder.exists() && !homeFolder.mkdir())
        throw new IllegalStateException("fail to create folder on " + homeFolder.getAbsolutePath());
      if (!homeFolder.isDirectory())
        throw new IllegalArgumentException(homeFolder.getAbsolutePath() + " is not folder");
    }

    public FtpServer build() {
      checkArguments();
      var userManagerFactory = new PropertiesUserManagerFactory();
      var userManager = userManagerFactory.createUserManager();
      var _user = new BaseUser();
      _user.setName(user);
      _user.setAuthorities(List.of(new WritePermission()));
      _user.setEnabled(true);
      _user.setPassword(password);
      _user.setHomeDirectory(homeFolder.getAbsolutePath());
      try {
        userManager.save(_user);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      var listenerFactory = new ListenerFactory();
      listenerFactory.setPort(controlPort);
      var connectionConfig = new DataConnectionConfigurationFactory();

      List<Integer> availableDataPorts =
          dataPorts.stream().map(Utils::resolvePort).collect(Collectors.toUnmodifiableList());

      connectionConfig.setActiveEnabled(false);
      connectionConfig.setPassiveExternalAddress(Utils.hostname());
      connectionConfig.setPassivePorts(
          availableDataPorts.stream().map(String::valueOf).collect(Collectors.joining(",")));
      listenerFactory.setDataConnectionConfiguration(
          connectionConfig.createDataConnectionConfiguration());

      var listener = listenerFactory.createListener();
      var factory = new FtpServerFactory();
      factory.setUserManager(userManager);
      factory.addListener("default", listener);
      var connectionConfigFactory = new ConnectionConfigFactory();
      // we disallow user to access ftp server by anonymous
      connectionConfigFactory.setAnonymousLoginEnabled(false);
      factory.setConnectionConfig(connectionConfigFactory.createConnectionConfig());
      var server = factory.createServer();
      try {
        server.start();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      return new FtpServer() {

        @Override
        public void close() {
          server.stop();
          //          Utils.delete(homeFolder);
        }

        @Override
        public String hostname() {
          return Utils.hostname();
        }

        @Override
        public int port() {
          return listener.getPort();
        }

        @Override
        public String user() {
          return _user.getName();
        }

        @Override
        public String password() {
          return _user.getPassword();
        }

        @Override
        public List<Integer> dataPorts() {
          return Stream.of(connectionConfig.getPassivePorts().split(","))
              .map(Integer::valueOf)
              .collect(Collectors.toUnmodifiableList());
        }

        @Override
        public String absolutePath() {
          return homeFolder.getAbsolutePath();
        }
      };
    }
  }
}
