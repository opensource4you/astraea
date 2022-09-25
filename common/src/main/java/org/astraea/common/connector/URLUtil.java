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
package org.astraea.common.connector;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.stream.Collectors;

public class URLUtil {
  public static URL getQueryUrl(URL url, Map<String, String> parameters)
      throws URISyntaxException, MalformedURLException {
    var uri = url.toURI();
    var queryString =
        parameters.entrySet().stream()
            .map(
                x -> {
                  var key = x.getKey();
                  var value = URLEncoder.encode(x.getValue(), StandardCharsets.UTF_8);
                  return key + "=" + value;
                })
            .collect(Collectors.joining("&"));

    return new URI(
            uri.getScheme(), uri.getAuthority(), uri.getPath(), queryString, uri.getFragment())
        .toURL();
  }
}
