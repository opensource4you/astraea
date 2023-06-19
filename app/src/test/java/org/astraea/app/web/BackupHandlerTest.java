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
package org.astraea.app.web;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.astraea.common.Configuration;
import org.astraea.common.Utils;
import org.astraea.common.admin.Admin;
import org.astraea.common.connector.ConnectorClient;
import org.astraea.common.connector.ConnectorConfigs;
import org.astraea.common.http.HttpExecutor;
import org.astraea.common.json.TypeRef;
import org.astraea.connector.backup.Exporter;
import org.astraea.connector.backup.Importer;
import org.astraea.it.Service;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class BackupHandlerTest {
  private static final Service SERVICE =
      Service.builder().numberOfBrokers(3).numberOfWorkers(3).build();

  @AfterAll
  static void closeService() {
    SERVICE.close();
  }

  @Test
  void testWithWebService() {
    try (var admin = Admin.of(SERVICE.bootstrapServers())) {
      var exporterName = Utils.randomString(10);
      var exporterPath = Utils.randomString(10);
      var topicName = Utils.randomString(10);
      var request = new BackupHandler.BackupRequest();
      var exporterConfig = new BackupHandler.ExporterConfig();
      exporterConfig.name = exporterName;
      exporterConfig.fsSchema = "local";
      exporterConfig.tasksMax = "3";
      exporterConfig.path = "/tmp/" + exporterPath;
      exporterConfig.topics = topicName;
      request.exporter = List.of(exporterConfig);
      request.importer = List.of();

      admin
          .creator()
          .topic(topicName)
          .numberOfPartitions(3)
          .numberOfReplicas((short) 1)
          .run()
          .toCompletableFuture()
          .join();
      Utils.sleep(Duration.ofSeconds(3));

      try (var service =
          new WebService(
              Admin.of(SERVICE.bootstrapServers()),
              0,
              id -> SERVICE.jmxServiceURL().getPort(),
              Duration.ofMillis(5),
              Configuration.EMPTY,
              List.copyOf(SERVICE.workerUrls()))) {
        var response =
            HttpExecutor.builder()
                .build()
                .post(
                    "http://localhost:" + service.port() + "/backups",
                    request,
                    TypeRef.of(BackupHandler.ConnectorInfoResponse.class))
                .toCompletableFuture()
                .join();

        var responseExporter = response.body().exporters.get(0);
        Assertions.assertEquals(200, response.statusCode());
        Assertions.assertEquals(exporterConfig.name, responseExporter.name);
        Assertions.assertEquals(
            exporterConfig.topics, responseExporter.config.get(ConnectorConfigs.TOPICS_KEY));
        Assertions.assertEquals(
            exporterConfig.tasksMax, responseExporter.config.get(ConnectorConfigs.TASK_MAX_KEY));
        Assertions.assertEquals(exporterConfig.fsSchema, responseExporter.config.get("fs.schema"));
        Assertions.assertEquals(exporterConfig.path, responseExporter.config.get("path"));
        Assertions.assertEquals(0, response.body().importers.size());

        HttpExecutor.builder()
            .build()
            .delete("http://localhost:" + service.port() + "/backups/" + exporterName)
            .toCompletableFuture()
            .join();
      }
    }
  }

  @Test
  void testGetWithoutQuery() {
    try (var admin = Admin.of(SERVICE.bootstrapServers())) {
      var topicName = Utils.randomString(10);
      var importerName = Utils.randomString(10);
      var exporterName = Utils.randomString(10);
      var pathName = org.astraea.it.Utils.createTempDirectory(Utils.randomString(10)).toString();
      var fsSchema = "local";
      var tasksMax = "3";
      var cleanSourcePolicy = "off";
      var connectorClient = ConnectorClient.builder().urls(SERVICE.workerUrls()).build();

      admin
          .creator()
          .topic(topicName)
          .numberOfPartitions(3)
          .numberOfReplicas((short) 1)
          .run()
          .toCompletableFuture()
          .join();
      Utils.sleep(Duration.ofSeconds(3));

      connectorClient
          .createConnector(
              importerName,
              Map.of(
                  "fs.schema",
                  fsSchema,
                  "path",
                  pathName,
                  "clean.source",
                  cleanSourcePolicy,
                  ConnectorConfigs.CONNECTOR_CLASS_KEY,
                  Importer.class.getName(),
                  ConnectorConfigs.TASK_MAX_KEY,
                  tasksMax))
          .toCompletableFuture()
          .join();
      connectorClient
          .createConnector(
              exporterName,
              Map.of(
                  "fs.schema",
                  fsSchema,
                  "path",
                  pathName,
                  ConnectorConfigs.CONNECTOR_CLASS_KEY,
                  Exporter.class.getName(),
                  ConnectorConfigs.TASK_MAX_KEY,
                  tasksMax,
                  ConnectorConfigs.TOPICS_KEY,
                  topicName,
                  ConnectorConfigs.KEY_CONVERTER_KEY,
                  ConnectorConfigs.BYTE_ARRAY_CONVERTER_CLASS,
                  ConnectorConfigs.VALUE_CONVERTER_KEY,
                  ConnectorConfigs.BYTE_ARRAY_CONVERTER_CLASS,
                  ConnectorConfigs.HEADER_CONVERTER_KEY,
                  ConnectorConfigs.BYTE_ARRAY_CONVERTER_CLASS))
          .toCompletableFuture()
          .join();

      var handler = new BackupHandler(connectorClient);
      var response =
          Assertions.assertInstanceOf(
              BackupHandler.ConnectorStatusResponse.class,
              handler.get(Channel.EMPTY).toCompletableFuture().join());
      var importer = response.importers.get(0);
      var exporter = response.exporters.get(0);

      Assertions.assertEquals(1, response.importers.size());
      Assertions.assertEquals(importerName, importer.name);
      Assertions.assertEquals(tasksMax, importer.configs.get(ConnectorConfigs.TASK_MAX_KEY));
      Assertions.assertEquals(fsSchema, importer.configs.get("fs.schema"));
      Assertions.assertEquals(pathName, importer.configs.get("path"));
      Assertions.assertEquals(cleanSourcePolicy, importer.configs.get("clean.source"));
      Assertions.assertEquals(1, response.exporters.size());
      Assertions.assertEquals(exporterName, response.exporters.get(0).name);
      Assertions.assertEquals(topicName, exporter.configs.get(ConnectorConfigs.TOPICS_KEY));
      Assertions.assertEquals(tasksMax, exporter.configs.get(ConnectorConfigs.TASK_MAX_KEY));
      Assertions.assertEquals(fsSchema, exporter.configs.get("fs.schema"));
      Assertions.assertEquals(pathName, exporter.configs.get("path"));

      connectorClient.deleteConnector(importerName).toCompletableFuture().join();
      connectorClient.deleteConnector(exporterName).toCompletableFuture().join();
    }
  }

  @Test
  void testGetWithQuery() {
    var firstImporterName = Utils.randomString(10);
    var secondImporterName = Utils.randomString(10);
    var pathName = org.astraea.it.Utils.createTempDirectory(Utils.randomString(10)).toString();
    var fsSchema = "local";
    var tasksMax = "3";
    var cleanSourcePolicy = "off";
    var connectorClient = ConnectorClient.builder().urls(SERVICE.workerUrls()).build();

    connectorClient
        .createConnector(
            firstImporterName,
            Map.of(
                "fs.schema",
                fsSchema,
                "path",
                pathName,
                "clean.source",
                cleanSourcePolicy,
                ConnectorConfigs.CONNECTOR_CLASS_KEY,
                Importer.class.getName(),
                ConnectorConfigs.TASK_MAX_KEY,
                tasksMax))
        .toCompletableFuture()
        .join();
    connectorClient
        .createConnector(
            secondImporterName,
            Map.of(
                "fs.schema",
                fsSchema,
                "path",
                pathName,
                "clean.source",
                cleanSourcePolicy,
                ConnectorConfigs.CONNECTOR_CLASS_KEY,
                Importer.class.getName(),
                ConnectorConfigs.TASK_MAX_KEY,
                tasksMax))
        .toCompletableFuture()
        .join();

    var handler = new BackupHandler(connectorClient);
    var response =
        Assertions.assertInstanceOf(
            BackupHandler.ConnectorStatusResponse.class,
            handler.get(Channel.ofTarget(firstImporterName)).toCompletableFuture().join());
    var importer = response.importers.get(0);

    Assertions.assertEquals(
        2, connectorClient.connectorNames().toCompletableFuture().join().size());
    Assertions.assertEquals(1, response.importers.size());
    Assertions.assertEquals(firstImporterName, importer.name);
    Assertions.assertEquals(tasksMax, importer.configs.get(ConnectorConfigs.TASK_MAX_KEY));
    Assertions.assertEquals(fsSchema, importer.configs.get("fs.schema"));
    Assertions.assertEquals(pathName, importer.configs.get("path"));
    Assertions.assertEquals(cleanSourcePolicy, importer.configs.get("clean.source"));

    connectorClient.deleteConnector(firstImporterName).toCompletableFuture().join();
    connectorClient.deleteConnector(secondImporterName).toCompletableFuture().join();
  }

  @Test
  void testPostSingleBackupOperation() {
    var importerName = Utils.randomString(10);
    var pathName = org.astraea.it.Utils.createTempDirectory(Utils.randomString(10)).toString();
    var fsSchema = "local";
    var tasksMax = "3";
    var cleanSourcePolicy = "off";
    var connectorClient = ConnectorClient.builder().urls(SERVICE.workerUrls()).build();
    var beforePostSize = connectorClient.connectorNames().toCompletableFuture().join().size();
    var handler = new BackupHandler(connectorClient);
    var request =
        Channel.ofRequest(
            String.format(
                "{\"importer\":"
                    + "[{\"name\":\"%s\",\"fsSchema\":\"%s\",\"tasksMax\":\"%s\","
                    + "\"path\":\"%s\",\"cleanSourcePolicy\":\"%s\"}]}",
                importerName, fsSchema, tasksMax, pathName, cleanSourcePolicy));
    var response =
        Assertions.assertInstanceOf(
            BackupHandler.ConnectorInfoResponse.class,
            handler.post(request).toCompletableFuture().join());
    var importer = response.importers.get(0);
    var afterPostSize = connectorClient.connectorNames().toCompletableFuture().join().size();
    Assertions.assertEquals(0, beforePostSize);
    Assertions.assertEquals(1, afterPostSize);
    Assertions.assertEquals(importerName, importer.name);
    Assertions.assertEquals(pathName, importer.config.get("path"));
    Assertions.assertEquals(fsSchema, importer.config.get("fs.schema"));
    Assertions.assertEquals(tasksMax, importer.config.get(ConnectorConfigs.TASK_MAX_KEY));
    Assertions.assertEquals(cleanSourcePolicy, importer.config.get("clean.source"));

    connectorClient.deleteConnector(importerName).toCompletableFuture().join();
  }

  @Test
  void testPostBackupOperations() {
    var firstImporterName = Utils.randomString(10);
    var secondImporterName = Utils.randomString(10);
    var firstExporterName = Utils.randomString(10);
    var secondExporterName = Utils.randomString(10);
    var topicName = Utils.randomString(10);
    var pathName = org.astraea.it.Utils.createTempDirectory(Utils.randomString(10)).toString();
    var fsSchema = "local";
    var tasksMax = "3";
    var cleanSourcePolicy = "off";
    var connectorClient = ConnectorClient.builder().urls(SERVICE.workerUrls()).build();
    var handler = new BackupHandler(connectorClient);
    var request =
        Channel.ofRequest(
            String.format(
                "{\"importer\":"
                    + "[{\"name\":\"%s\",\"fsSchema\":\"%s\",\"tasksMax\":\"%s\",\"path\":\"%s\","
                    + "\"cleanSourcePolicy\":\"%s\"},"
                    + "{\"name\":\"%s\",\"fsSchema\":\"%s\",\"tasksMax\":\"%s\",\"path\":\"%s\","
                    + "\"cleanSourcePolicy\":\"%s\"}],"
                    + "\"exporter\":"
                    + "[{\"name\":\"%s\",\"fsSchema\":\"%s\",\"tasksMax\":\"%s\",\"path\":\"%s\","
                    + "\"topics\":\"%s\"},"
                    + "{\"name\":\"%s\",\"fsSchema\":\"%s\",\"tasksMax\":\"%s\",\"path\":\"%s\","
                    + "\"topics\":\"%s\"}]}",
                firstImporterName,
                fsSchema,
                tasksMax,
                pathName,
                cleanSourcePolicy,
                secondImporterName,
                fsSchema,
                tasksMax,
                pathName,
                cleanSourcePolicy,
                firstExporterName,
                fsSchema,
                tasksMax,
                pathName,
                topicName,
                secondExporterName,
                fsSchema,
                tasksMax,
                pathName,
                topicName));
    var response =
        Assertions.assertInstanceOf(
            BackupHandler.ConnectorInfoResponse.class,
            handler.post(request).toCompletableFuture().join());
    Assertions.assertEquals(2, response.importers.size());
    Assertions.assertEquals(2, response.exporters.size());
    Assertions.assertTrue(
        response.importers.stream()
            .map(importer -> importer.name)
            .anyMatch(name -> List.of(firstImporterName, secondImporterName).contains(name)));
    response.importers.forEach(
        importer -> {
          Assertions.assertEquals(pathName, importer.config.get("path"));
          Assertions.assertEquals(fsSchema, importer.config.get("fs.schema"));
          Assertions.assertEquals(tasksMax, importer.config.get(ConnectorConfigs.TASK_MAX_KEY));
          Assertions.assertEquals(cleanSourcePolicy, importer.config.get("clean.source"));
        });
    Assertions.assertTrue(
        response.exporters.stream()
            .map(exporter -> exporter.name)
            .anyMatch(name -> List.of(firstExporterName, secondExporterName).contains(name)));
    response.exporters.forEach(
        exporter -> {
          Assertions.assertEquals(topicName, exporter.config.get(ConnectorConfigs.TOPICS_KEY));
          Assertions.assertEquals(tasksMax, exporter.config.get(ConnectorConfigs.TASK_MAX_KEY));
          Assertions.assertEquals(fsSchema, exporter.config.get("fs.schema"));
          Assertions.assertEquals(pathName, exporter.config.get("path"));
        });

    connectorClient.deleteConnector(firstImporterName).toCompletableFuture().join();
    connectorClient.deleteConnector(secondImporterName).toCompletableFuture().join();
    connectorClient.deleteConnector(firstExporterName).toCompletableFuture().join();
    connectorClient.deleteConnector(secondExporterName).toCompletableFuture().join();
  }

  @Test
  void testDelete() {
    var importerName = Utils.randomString(10);
    var pathName = org.astraea.it.Utils.createTempDirectory(Utils.randomString(10)).toString();
    var fsSchema = "local";
    var tasksMax = "3";
    var cleanSourcePolicy = "off";
    var connectorClient = ConnectorClient.builder().urls(SERVICE.workerUrls()).build();

    connectorClient
        .createConnector(
            importerName,
            Map.of(
                "fs.schema",
                fsSchema,
                "path",
                pathName,
                "clean.source",
                cleanSourcePolicy,
                ConnectorConfigs.CONNECTOR_CLASS_KEY,
                Importer.class.getName(),
                ConnectorConfigs.TASK_MAX_KEY,
                tasksMax))
        .toCompletableFuture()
        .join();

    var beforeDeleteSize = connectorClient.connectorNames().toCompletableFuture().join().size();
    var handler = new BackupHandler(connectorClient);
    handler.delete(Channel.ofTarget(importerName)).toCompletableFuture().join();
    var afterDeleteSize = connectorClient.connectorNames().toCompletableFuture().join().size();

    Assertions.assertEquals(1, beforeDeleteSize);
    Assertions.assertEquals(0, afterDeleteSize);
  }

  @Test
  void testPutSingleBackupOperation() {
    var importerName = Utils.randomString(10);
    var pathName = org.astraea.it.Utils.createTempDirectory(Utils.randomString(10)).toString();
    var fsSchema = "local";
    var tasksMax = "3";
    var cleanSourcePolicy = "off";
    var connectorClient = ConnectorClient.builder().urls(SERVICE.workerUrls()).build();

    connectorClient
        .createConnector(
            importerName,
            Map.of(
                "fs.schema",
                fsSchema,
                "path",
                pathName,
                "clean.source",
                cleanSourcePolicy,
                ConnectorConfigs.CONNECTOR_CLASS_KEY,
                Importer.class.getName(),
                ConnectorConfigs.TASK_MAX_KEY,
                tasksMax))
        .toCompletableFuture()
        .join();

    var newTasksMax = "5";
    var newPathName = org.astraea.it.Utils.createTempDirectory(Utils.randomString(10)).toString();
    var newCleanSourcePolicy = "delete";
    var request =
        Channel.ofRequest(
            String.format(
                "{\"importer\":"
                    + "[{\"name\":\"%s\",\"fsSchema\":\"%s\",\"tasksMax\":\"%s\","
                    + "\"path\":\"%s\",\"cleanSourcePolicy\":\"%s\"}]}",
                importerName, fsSchema, newTasksMax, newPathName, newCleanSourcePolicy));
    var handler = new BackupHandler(connectorClient);
    var response =
        Assertions.assertInstanceOf(
            BackupHandler.ConnectorInfoResponse.class,
            handler.put(request).toCompletableFuture().join());
    var importer = response.importers.get(0);

    Assertions.assertEquals(
        1, connectorClient.connectorNames().toCompletableFuture().join().size());
    Assertions.assertEquals(importerName, importer.name);
    Assertions.assertEquals(newPathName, importer.config.get("path"));
    Assertions.assertEquals(fsSchema, importer.config.get("fs.schema"));
    Assertions.assertEquals(newTasksMax, importer.config.get(ConnectorConfigs.TASK_MAX_KEY));
    Assertions.assertEquals(newCleanSourcePolicy, importer.config.get("clean.source"));

    connectorClient.deleteConnector(importerName).toCompletableFuture().join();
  }

  @Test
  void testPutBackupOperations() {
    var firstImporterName = Utils.randomString(10);
    var secondImporterName = Utils.randomString(10);
    var pathName = org.astraea.it.Utils.createTempDirectory(Utils.randomString(10)).toString();
    var fsSchema = "local";
    var tasksMax = "3";
    var cleanSourcePolicy = "off";
    var connectorClient = ConnectorClient.builder().urls(SERVICE.workerUrls()).build();

    connectorClient.createConnector(
        firstImporterName,
        Map.of(
            "fs.schema",
            fsSchema,
            "path",
            pathName,
            "clean.source",
            cleanSourcePolicy,
            ConnectorConfigs.CONNECTOR_CLASS_KEY,
            Importer.class.getName(),
            ConnectorConfigs.TASK_MAX_KEY,
            tasksMax));
    connectorClient.createConnector(
        secondImporterName,
        Map.of(
            "fs.schema",
            fsSchema,
            "path",
            pathName,
            "clean.source",
            cleanSourcePolicy,
            ConnectorConfigs.CONNECTOR_CLASS_KEY,
            Importer.class.getName(),
            ConnectorConfigs.TASK_MAX_KEY,
            tasksMax));

    var newTasksMax = "5";
    var newPathName = org.astraea.it.Utils.createTempDirectory(Utils.randomString(10)).toString();
    var newCleanSourcePolicy = "delete";
    var handler = new BackupHandler(connectorClient);
    var request =
        Channel.ofRequest(
            String.format(
                "{\"importer\":"
                    + "[{\"name\":\"%s\",\"fsSchema\":\"%s\",\"tasksMax\":\"%s\",\"path\":\"%s\","
                    + "\"cleanSourcePolicy\":\"%s\"},"
                    + "{\"name\":\"%s\",\"fsSchema\":\"%s\",\"tasksMax\":\"%s\",\"path\":\"%s\","
                    + "\"cleanSourcePolicy\":\"%s\"}]}",
                firstImporterName,
                fsSchema,
                newTasksMax,
                newPathName,
                newCleanSourcePolicy,
                secondImporterName,
                fsSchema,
                newTasksMax,
                newPathName,
                newCleanSourcePolicy));
    var response =
        Assertions.assertInstanceOf(
            BackupHandler.ConnectorInfoResponse.class,
            handler.put(request).toCompletableFuture().join());
    Assertions.assertEquals(2, response.importers.size());
    Assertions.assertTrue(
        response.importers.stream()
            .map(importer -> importer.name)
            .anyMatch(name -> List.of(firstImporterName, secondImporterName).contains(name)));
    response.importers.forEach(
        importer -> {
          Assertions.assertEquals(newPathName, importer.config.get("path"));
          Assertions.assertEquals(fsSchema, importer.config.get("fs.schema"));
          Assertions.assertEquals(newTasksMax, importer.config.get(ConnectorConfigs.TASK_MAX_KEY));
          Assertions.assertEquals(newCleanSourcePolicy, importer.config.get("clean.source"));
        });

    connectorClient.deleteConnector(firstImporterName).toCompletableFuture().join();
    connectorClient.deleteConnector(secondImporterName).toCompletableFuture().join();
  }
}
