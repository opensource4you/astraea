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

import java.util.List;
import javafx.application.Application;
import javafx.geometry.Side;
import javafx.scene.Scene;
import javafx.stage.Stage;
import org.astraea.gui.pane.TabPane;
import org.astraea.gui.tab.AboutTab;
import org.astraea.gui.tab.BalancerTab;
import org.astraea.gui.tab.BrokerTab;
import org.astraea.gui.tab.ClientTab;
import org.astraea.gui.tab.QuotaTab;
import org.astraea.gui.tab.SettingTab;
import org.astraea.gui.tab.topic.TopicTab;

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
      stage.setTitle("Astraea");
      stage.setHeight(900);
      stage.setWidth(1200);
      stage.setScene(
          new Scene(
              TabPane.of(
                  Side.BOTTOM,
                  List.of(
                      SettingTab.of(context),
                      BrokerTab.of(context),
                      TopicTab.of(context),
                      ClientTab.of(context),
                      QuotaTab.of(context),
                      BalancerTab.of(context),
                      AboutTab.of(context))),
              300,
              300));
      stage.show();
    }

    @Override
    public void stop() {}
  }
}
