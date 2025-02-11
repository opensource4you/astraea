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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javafx.geometry.Side;
import javafx.scene.Node;
import org.apache.kafka.common.network.ListenerName;
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
  private static final Pattern URI_PARSE_REGEXP =
      Pattern.compile("^(.*)://\\[?([0-9a-zA-Z\\-%._:]*)\\]?:(-?[0-9]+)");

  private static Node addVoterNode(Context context) {

    var idKey = "node id";
    var multiInput =
        List.of(TextInput.required(idKey, EditableText.singleLine().disallowEmpty().build()));
    var firstPart =
        FirstPart.builder()
            .textInputs(multiInput)
            .clickName("add voter")
            .tableRefresher(
                (argument, logger) ->
                    context
                        .admin()
                        .quorumInfo()
                        .thenCompose(
                            quorumInfo ->
                                context
                                    .admin()
                                    .controllers()
                                    .thenCompose(
                                        controllers -> {
                                          var id =
                                              Integer.parseInt(argument.nonEmptyTexts().get(idKey));
                                          var directoryId =
                                              quorumInfo.observers().stream()
                                                  .filter(e -> e.replicaId() == id)
                                                  .findFirst()
                                                  .get()
                                                  .replicaDirectoryId();
                                          var controller =
                                              controllers.stream()
                                                  .filter(e -> e.id() == id)
                                                  .findFirst()
                                                  .get();
                                          var endpoints = raftEndpoints(controller.config().raw());
                                          return context
                                              .admin()
                                              .addVoter(id, directoryId.toString(), endpoints);
                                        }))
                        .thenApply(
                            ignored -> {
                              logger.log(
                                  "succeed to add "
                                      + argument.nonEmptyTexts().get(idKey)
                                      + " to the voters");
                              return List.of();
                            }))
            .build();
    return PaneBuilder.of().firstPart(firstPart).build();
  }

  private static Node removeVoterNode(Context context) {
    var idKey = "node id";
    var multiInput =
        List.of(TextInput.required(idKey, EditableText.singleLine().disallowEmpty().build()));

    var firstPart =
        FirstPart.builder()
            .textInputs(multiInput)
            .clickName("remove voter")
            .tableRefresher(
                (argument, logger) ->
                    context
                        .admin()
                        .quorumInfo()
                        .thenCompose(
                            quorumInfo ->
                                context
                                    .admin()
                                    .removeVoter(
                                        Integer.parseInt(argument.nonEmptyTexts().get(idKey)),
                                        Stream.concat(
                                                quorumInfo.voters().stream(),
                                                quorumInfo.observers().stream())
                                            .filter(
                                                e ->
                                                    e.replicaId()
                                                        == Integer.parseInt(
                                                            argument.nonEmptyTexts().get(idKey)))
                                            .findFirst()
                                            .get()
                                            .replicaDirectoryId()
                                            .toString()))
                        .thenApply(
                            ignored -> {
                              logger.log(
                                  "succeed to remove "
                                      + argument.nonEmptyTexts().get(idKey)
                                      + " from the voters");
                              return List.of();
                            }))
            .build();
    return PaneBuilder.of().firstPart(firstPart).build();
  }

  private static List<Map<String, Object>> basicResult(
      QuorumInfo quorumInfo, List<Controller> controllers) {

    Function<Integer, String> endpointString =
        id -> {
          var r = quorumInfo.endpoints().getOrDefault(id, List.of());
          if (r.isEmpty()) {
            r =
                controllers.stream()
                    .filter(c -> c.id() == id)
                    .findFirst()
                    .map(c -> raftEndpoints(c.config().raw()))
                    .orElse(List.of());
          }
          return r.stream()
              .map(e -> e.name() + "@" + e.host() + ":" + e.port())
              .collect(Collectors.joining(","));
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
                        endpointString.apply(rs.replicaId())))
            .toList());

    result.addAll(
        quorumInfo.observers().stream()
            .map(
                rs ->
                    MapUtils.<String, Object>of(
                        "role",
                        controllers.stream().anyMatch(b -> b.id() == rs.replicaId())
                            ? "observer"
                            : "broker",
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
                        endpointString.apply(rs.replicaId())))
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

  static List<RaftEndpoint> raftEndpoints(Map<String, String> props) {
    var listeners =
        listenerToRaftEndpoint(props.getOrDefault("listeners", "")).stream()
            .collect(Collectors.toMap(RaftEndpoint::name, e -> e));
    var advertisedListeners =
        listenerToRaftEndpoint(props.getOrDefault("advertised.listeners", "")).stream()
            .collect(Collectors.toMap(RaftEndpoint::name, e -> e));
    if (!props.containsKey("controller.listener.names")) {
      throw new RuntimeException("controller.listener.names was not found");
    }
    return Arrays.stream(props.get("controller.listener.names").split(","))
        .map(name -> ListenerName.normalised(name).value())
        .map(
            name ->
                Objects.requireNonNull(
                    advertisedListeners.getOrDefault(name, listeners.get(name)),
                    "Cannot find information about controller listener name: " + name))
        .toList();
  }

  private static List<RaftEndpoint> listenerToRaftEndpoint(String input) {
    return parseCsvList(input.trim()).stream()
        .map(
            entry -> {
              Matcher matcher = URI_PARSE_REGEXP.matcher(entry);
              if (!matcher.matches()) {
                throw new RuntimeException("Unable to parse " + entry + " to a broker endpoint");
              }
              ListenerName listenerName = ListenerName.normalised(matcher.group(1));
              String host = matcher.group(2);
              if (host.isEmpty()) {
                // By Kafka convention, an empty host string indicates binding to the wildcard
                // address, and is stored as null.
                host = null;
              }
              String portString = matcher.group(3);
              return new RaftEndpoint(listenerName.value(), host, Integer.parseInt(portString));
            })
        .toList();
  }

  private static List<String> parseCsvList(String csvList) {
    if (csvList == null || csvList.isEmpty()) {
      return Collections.emptyList();
    } else {
      return Stream.of(csvList.split("\\s*,\\s*"))
          .filter(v -> !v.isEmpty())
          .collect(Collectors.toList());
    }
  }
}
