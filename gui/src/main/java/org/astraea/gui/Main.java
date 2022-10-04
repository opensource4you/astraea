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
package org.astraea.gui;

import javafx.application.Application;
import javafx.geometry.Side;
import javafx.scene.Scene;
import javafx.scene.control.TabPane;
import javafx.stage.Stage;

/**
 * Since the Java launcher checks if the main class extends javafx.application.Application, and in
 * that case it requires the JavaFX runtime available as modules (not as jars), a possible
 * workaround to make it work, should be adding a new Main class that will be the main class of your
 * project, and that class will be the one that calls your JavaFX Application class. FROM <a
 * href="https://stackoverflow.com/questions/52569724/javafx-11-create-a-jar-file-with-gradle">...</a>
 */
public class Main {
  public static void main(String[] args) {
    Application.launch(App.class, args);
  }

  public static class App extends Application {

    @Override
    public void start(Stage stage) {
      var context = new Context();
      var rootPane =
          new TabPane(
              SettingTab.of(context),
              BrokerTab.of(context),
              TopicTab.of(context),
              PartitionTab.of(context),
              ConfigTab.of(context),
              ConsumerTab.of(context),
              ProducerTab.of(context),
              TransactionTab.of(context),
              AddingReplicaTab.of(context),
              CreateTopicTab.of(context),
              ReassignReplicaTab.of(context),
              BalancerTab.of(context),
              AboutTab.of(context));
      rootPane.setSide(Side.TOP);
      rootPane.setTabClosingPolicy(TabPane.TabClosingPolicy.UNAVAILABLE);
      var scene = new Scene(rootPane, 300, 300);
      stage.setTitle("Astraea");
      stage.setHeight(1000);
      stage.setWidth(1000);
      stage.setScene(scene);
      stage.show();
    }

    @Override
    public void stop() {}
  }
}
