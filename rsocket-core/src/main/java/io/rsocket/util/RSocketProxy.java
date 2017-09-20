/*
 * Copyright 2016 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.rsocket.util;

import io.rsocket.Payload;
import io.rsocket.RSocket;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/** Wrapper/Proxy for a RSocket. This is useful when we want to override a specific method. */
public class RSocketProxy<T extends Payload> implements RSocket<T> {
  protected final RSocket<T> source;

  public RSocketProxy(RSocket<T> source) {
    this.source = source;
  }

  @Override
  public Mono<Void> fireAndForget(T payload) {
    return source.fireAndForget(payload);
  }

  @Override
  public Mono<T> requestResponse(T payload) {
    return source.requestResponse(payload);
  }

  @Override
  public Flux<T> requestStream(T payload) {
    return source.requestStream(payload);
  }

  @Override
  public Flux<T> requestChannel(Publisher<T> payloads) {
    return source.requestChannel(payloads);
  }

  @Override
  public Mono<Void> metadataPush(T payload) {
    return source.metadataPush(payload);
  }

  @Override
  public double availability() {
    return source.availability();
  }

  @Override
  public Mono<Void> close() {
    return source.close();
  }

  @Override
  public Mono<Void> onClose() {
    return source.onClose();
  }
}
