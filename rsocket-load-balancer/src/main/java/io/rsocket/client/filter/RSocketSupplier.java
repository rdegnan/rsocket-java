/*
 * Copyright 2016 Netflix, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package io.rsocket.client.filter;

import io.rsocket.Availability;
import io.rsocket.Closeable;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.stat.Ewma;
import io.rsocket.util.Clock;
import io.rsocket.util.RSocketProxy;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

/** */
public class RSocketSupplier<T extends Payload>
    implements Availability, Supplier<Mono<RSocket<T>>>, Closeable {

  private static final double EPSILON = 1e-4;

  private Supplier<Mono<RSocket<T>>> rSocketSupplier;

  private final MonoProcessor<Void> onClose;

  private final long tau;
  private long stamp;
  private final Ewma errorPercentage;

  public RSocketSupplier(Supplier<Mono<RSocket<T>>> rSocketSupplier, long halfLife, TimeUnit unit) {
    this.rSocketSupplier = rSocketSupplier;
    this.tau = Clock.unit().convert((long) (halfLife / Math.log(2)), unit);
    this.stamp = Clock.now();
    this.errorPercentage = new Ewma(halfLife, unit, 1.0);
    this.onClose = MonoProcessor.create();
  }

  public RSocketSupplier(Supplier<Mono<RSocket<T>>> rSocketSupplier) {
    this(rSocketSupplier, 5, TimeUnit.SECONDS);
  }

  @Override
  public double availability() {
    double e = errorPercentage.value();
    if (Clock.now() - stamp > tau) {
      // If the window is expired artificially increase the availability
      double a = Math.min(1.0, e + 0.5);
      errorPercentage.reset(a);
    }
    if (e < EPSILON) {
      e = 0.0;
    } else if (1.0 - EPSILON < e) {
      e = 1.0;
    }

    return e;
  }

  private synchronized void updateErrorPercentage(double value) {
    errorPercentage.insert(value);
    stamp = Clock.now();
  }

  @Override
  public Mono<RSocket<T>> get() {
    return rSocketSupplier
        .get()
        .doOnNext(o -> updateErrorPercentage(1.0))
        .doOnError(t -> updateErrorPercentage(0.0))
        .map(AvailabilityAwareRSocketProxy::new);
  }

  @Override
  public Mono<Void> close() {
    return Mono.empty().doFinally(s -> onClose.onComplete()).then();
  }

  @Override
  public Mono<Void> onClose() {
    return onClose;
  }

  private class AvailabilityAwareRSocketProxy extends RSocketProxy<T> {
    public AvailabilityAwareRSocketProxy(RSocket source) {
      super(source);

      onClose.then(close()).subscribe();
    }

    @Override
    public Mono<Void> fireAndForget(T payload) {
      return source
          .fireAndForget(payload)
          .doOnError(t -> errorPercentage.insert(0.0))
          .doOnSuccess(v -> updateErrorPercentage(1.0));
    }

    @Override
    public Mono<T> requestResponse(T payload) {
      return source
          .requestResponse(payload)
          .doOnError(t -> errorPercentage.insert(0.0))
          .doOnSuccess(p -> updateErrorPercentage(1.0));
    }

    @Override
    public Flux<T> requestStream(T payload) {
      return source
          .requestStream(payload)
          .doOnError(th -> errorPercentage.insert(0.0))
          .doOnComplete(() -> updateErrorPercentage(1.0));
    }

    @Override
    public Flux<T> requestChannel(Publisher<T> payloads) {
      return source
          .requestChannel(payloads)
          .doOnError(th -> errorPercentage.insert(0.0))
          .doOnComplete(() -> updateErrorPercentage(1.0));
    }

    @Override
    public Mono<Void> metadataPush(T payload) {
      return source
          .metadataPush(payload)
          .doOnError(t -> errorPercentage.insert(0.0))
          .doOnSuccess(v -> updateErrorPercentage(1.0));
    }

    @Override
    public double availability() {
      // If the window is expired set success and failure to zero and return
      // the child availability
      if (Clock.now() - stamp > tau) {
        updateErrorPercentage(1.0);
      }
      return source.availability() * errorPercentage.value();
    }
  }
}
