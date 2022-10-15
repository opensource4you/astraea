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
package org.astraea.gui.box;

import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.Node;

public class VBox extends javafx.scene.layout.VBox {

  public static VBox of(Node... nodes) {
    return of(Pos.CENTER_LEFT, nodes);
  }

  public static VBox of(Pos pos, Node... nodes) {
    var pane = new VBox(10);
    pane.setPadding(new Insets(15));
    pane.getChildren().setAll(nodes);
    pane.setAlignment(pos);
    return pane;
  }

  private VBox(double spacing) {
    super(spacing);
  }
}
