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
package org.astraea.gui.tab;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import javafx.geometry.Side;
import javafx.scene.Node;
import org.astraea.common.FutureUtils;
import org.astraea.common.MapUtils;
import org.astraea.common.admin.Controller;
import org.astraea.common.admin.QuorumInfo;
import org.astraea.common.admin.RaftEndpoint;
import org.astraea.gui.Context;
import org.astraea.gui.pane.FirstPart;
import org.astraea.gui.pane.PaneBuilder;
import org.astraea.gui.pane.Slide;
import org.astraea.gui.text.EditableText;
import org.astraea.gui.text.TextInput;

public class QuorumNode {

  private static Node addVoterNode(Context context) {
    var id = "node id";
    var directoryId = "directory id";
    var name = "endpoint name";
    var host = "endpoint host";
    var port = "endpoint port";
    var multiInput =
        List.of(
            TextInput.required(id, EditableText.singleLine().disallowEmpty().build()),
            TextInput.required(directoryId, EditableText.singleLine().disallowEmpty().build()),
            TextInput.required(name, EditableText.singleLine().disallowEmpty().build()),
            TextInput.required(host, EditableText.singleLine().disallowEmpty().build()),
            TextInput.required(port, EditableText.singleLine().onlyNumber().build()));
    var firstPart =
        FirstPart.builder()
            .textInputs(multiInput)
            .clickName("add voter")
            .tableRefresher(
                (argument, logger) ->
                    context
                        .admin()
                        .addVoter(
                            Integer.parseInt(argument.nonEmptyTexts().get(id)),
                            argument.nonEmptyTexts().get(directoryId),
                            new RaftEndpoint(
                                argument.nonEmptyTexts().get(name),
                                argument.nonEmptyTexts().get(host),
                                Integer.parseInt(argument.nonEmptyTexts().get(port))))
                        .thenApply(
                            ignored -> {
                              logger.log(
                                  "succeed to add "
                                      + argument.nonEmptyTexts().get(id)
                                      + " to the voters");
                              return List.of();
                            }))
            .build();
    return PaneBuilder.of().firstPart(firstPart).build();
  }

  private static Node removeVoterNode(Context context) {
    var id = "node id";
    var directoryId = "directory id";
    var multiInput =
        List.of(
            TextInput.required(id, EditableText.singleLine().disallowEmpty().build()),
            TextInput.required(directoryId, EditableText.singleLine().disallowEmpty().build()));
    var firstPart =
        FirstPart.builder()
            .textInputs(multiInput)
            .clickName("remove voter")
            .tableRefresher(
                (argument, logger) ->
                    context
                        .admin()
                        .removeVoter(
                            Integer.parseInt(argument.nonEmptyTexts().get(id)),
                            argument.nonEmptyTexts().get(directoryId))
                        .thenApply(
                            ignored -> {
                              logger.log(
                                  "succeed to remove "
                                      + argument.nonEmptyTexts().get(id)
                                      + " from the voters");
                              return List.of();
                            }))
            .build();
    return PaneBuilder.of().firstPart(firstPart).build();
  }

  private static List<Map<String, Object>> basicResult(
      QuorumInfo quorumInfo, List<Controller> controllers) {

    Function<Integer, String> endpoint =
        id -> {
          var r =
              quorumInfo.endpoints().getOrDefault(id, List.of()).stream()
                  .map(Object::toString)
                  .collect(Collectors.joining(","));
          if (r.isEmpty()) {
            r =
                controllers.stream()
                    .filter(c -> c.id() == id)
                    .map(Controller::toString)
                    .collect(Collectors.joining(","));
          }
          return r;
        };

    var result = new ArrayList<Map<String, Object>>();
    result.addAll(
        quorumInfo.voters().stream()
            .map(
                rs ->
                    MapUtils.<String, Object>of(
                        "role",
                        "voter",
                        "leader",
                        quorumInfo.leaderId() == rs.replicaId(),
                        "node id",
                        rs.replicaId(),
                        "directory id",
                        rs.replicaDirectoryId(),
                        "end offset",
                        rs.logEndOffset(),
                        "last fetch timestamp",
                        rs.lastFetchTimestamp().orElse(-1),
                        "last caught-up timestamp",
                        rs.lastCaughtUpTimestamp().orElse(-1),
                        "endpoints",
                        quorumInfo.endpoints().getOrDefault(rs.replicaId(), List.of()).stream()
                            .map(Record::toString)
                            .collect(Collectors.joining(","))))
            .toList());

    result.addAll(
        quorumInfo.observers().stream()
            .map(
                rs ->
                    MapUtils.<String, Object>of(
                        "role",
                        controllers.stream().anyMatch(b -> b.id() == rs.replicaId())
                            ? "observer"
                            : "observer (broker)",
                        "leader",
                        quorumInfo.leaderId() == rs.replicaId(),
                        "node id",
                        rs.replicaId(),
                        "directory id",
                        rs.replicaDirectoryId(),
                        "end offset",
                        rs.logEndOffset(),
                        "last fetch timestamp",
                        rs.lastFetchTimestamp().orElse(-1),
                        "last caught-up timestamp",
                        rs.lastCaughtUpTimestamp().orElse(-1),
                        "endpoints",
                        endpoint.apply(rs.replicaId())))
            .toList());
    return result;
  }

  private static Node basicNode(Context context) {
    var firstPart =
        FirstPart.builder()
            .clickName("REFRESH")
            .tableRefresher(
                (argument, logger) ->
                    FutureUtils.combine(
                        context.admin().quorumInfo(),
                        context.admin().controllers(),
                        QuorumNode::basicResult))
            .build();
    return PaneBuilder.of().firstPart(firstPart).build();
  }

  public static Node of(Context context) {
    return Slide.of(
            Side.TOP,
            MapUtils.of(
                "basic",
                basicNode(context),
                "add",
                addVoterNode(context),
                "remove",
                removeVoterNode(context)))
        .node();
  }
}
