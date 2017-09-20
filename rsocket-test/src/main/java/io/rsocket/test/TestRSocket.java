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

package io.rsocket.test;

import io.rsocket.AbstractRSocket;
import io.rsocket.util.PayloadImpl;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class TestRSocket extends AbstractRSocket<PayloadImpl> {

  @Override
  public Mono<PayloadImpl> requestResponse(PayloadImpl payload) {
    return Mono.just(new PayloadImpl("hello world", "metadata"));
  }

  @Override
  public Flux<PayloadImpl> requestStream(PayloadImpl payload) {
    return Flux.range(1, 10_000).flatMap(l -> requestResponse(payload));
  }

  @Override
  public Mono<Void> metadataPush(PayloadImpl payload) {
    return Mono.empty();
  }

  @Override
  public Mono<Void> fireAndForget(PayloadImpl payload) {
    return Mono.empty();
  }

  @Override
  public Flux<PayloadImpl> requestChannel(Publisher<PayloadImpl> payloads) {
    // TODO is defensive copy neccesary?
    return Flux.from(payloads).map(p -> new PayloadImpl(p.getDataUtf8(), p.getMetadataUtf8()));
  }
}
