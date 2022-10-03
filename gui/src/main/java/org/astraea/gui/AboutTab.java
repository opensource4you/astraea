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

import javafx.scene.control.Tab;
import org.astraea.common.VersionUtils;

public class AboutTab {

  public static Tab of(Context context) {
    var tab = new Tab("about");
    tab.setContent(
        Utils.vbox(
            Utils.hbox(Utils.label("Version:"), Utils.copyableField(VersionUtils.VERSION)),
            Utils.hbox(Utils.label("Revision:"), Utils.copyableField(VersionUtils.REVISION)),
            Utils.hbox(Utils.label("Build Date:"), Utils.copyableField(VersionUtils.DATE)),
            Utils.hbox(
                Utils.label("Web:"), Utils.copyableField("https://github.com/skiptests/astraea"))));
    return tab;
  }
}
