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
package org.astraea.common;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import org.astraea.common.function.Bi3Function;
import org.astraea.common.function.Bi4Function;
import org.astraea.common.function.Bi5Function;

public final class FutureUtils {

  public static <T> CompletableFuture<List<T>> sequence(Collection<CompletableFuture<T>> futures) {
    return CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[0]))
        .thenApply(f -> futures.stream().map(CompletableFuture::join).collect(Collectors.toList()));
  }

  public static <A, B, R> CompletionStage<R> combine(
      CompletionStage<? extends A> f0,
      CompletionStage<? extends B> f1,
      BiFunction<? super A, ? super B, ? extends R> fn) {
    return f0.thenCompose(a -> f1.thenApply(b -> fn.apply(a, b)));
  }

  public static <A, B, C, R> CompletionStage<R> combine(
      CompletionStage<? extends A> f0,
      CompletionStage<? extends B> f1,
      CompletionStage<? extends C> f2,
      Bi3Function<? super A, ? super B, ? super C, ? extends R> fn) {
    return f0.thenCompose(a -> f1.thenCompose(b -> f2.thenApply(c -> fn.apply(a, b, c))));
  }

  public static <A, B, C, D, R> CompletionStage<R> combine(
      CompletionStage<? extends A> f0,
      CompletionStage<? extends B> f1,
      CompletionStage<? extends C> f2,
      CompletionStage<? extends D> f3,
      Bi4Function<? super A, ? super B, ? super C, ? super D, ? extends R> fn) {
    return f0.thenCompose(
        a -> f1.thenCompose(b -> f2.thenCompose(c -> f3.thenApply(d -> fn.apply(a, b, c, d)))));
  }

  public static <A, B, C, D, E, R> CompletionStage<R> combine(
      CompletionStage<? extends A> f0,
      CompletionStage<? extends B> f1,
      CompletionStage<? extends C> f2,
      CompletionStage<? extends D> f3,
      CompletionStage<? extends E> f4,
      Bi5Function<? super A, ? super B, ? super C, ? super D, ? super E, ? extends R> fn) {
    return f0.thenCompose(
        a ->
            f1.thenCompose(
                b ->
                    f2.thenCompose(
                        c -> f3.thenCompose(d -> f4.thenApply(e -> fn.apply(a, b, c, d, e))))));
  }

  private FutureUtils() {}
}
