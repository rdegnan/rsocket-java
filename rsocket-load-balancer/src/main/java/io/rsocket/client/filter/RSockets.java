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

package io.rsocket.client.filter;

import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.util.RSocketProxy;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public final class RSockets {

  private RSockets() {
    // No Instances.
  }

  /**
   * Provides a mapping function to wrap a {@code RSocket} such that all requests will timeout, if
   * not completed after the specified {@code timeout}.
   *
   * @param timeout timeout duration.
   * @return Function to transform any socket into a timeout socket.
   */
  public static <T extends Payload> Function<RSocket<T>, RSocket<T>> timeout(Duration timeout) {
    return source ->
        new RSocketProxy<T>(source) {
          @Override
          public Mono<Void> fireAndForget(T payload) {
            return source.fireAndForget(payload).timeout(timeout);
          }

          @Override
          public Mono<T> requestResponse(T payload) {
            return source.requestResponse(payload).timeout(timeout);
          }

          @Override
          public Flux<T> requestStream(T payload) {
            return source.requestStream(payload).timeout(timeout);
          }

          @Override
          public Flux<T> requestChannel(Publisher<T> payloads) {
            return source.requestChannel(payloads).timeout(timeout);
          }

          @Override
          public Mono<Void> metadataPush(T payload) {
            return source.metadataPush(payload).timeout(timeout);
          }
        };
  }

  /**
   * Provides a mapping function to wrap a {@code RSocket} such that a call to {@link
   * RSocket#close()} does not cancel all pending requests. Instead, it will wait for all pending
   * requests to finish and then close the socket.
   *
   * @return Function to transform any socket into a safe closing socket.
   */
  public static <T extends Payload> Function<RSocket<T>, RSocket<T>> safeClose() {
    return source ->
        new RSocketProxy<T>(source) {
          final AtomicInteger count = new AtomicInteger();
          final AtomicBoolean closed = new AtomicBoolean();

          @Override
          public Mono<Void> fireAndForget(T payload) {
            return source
                .fireAndForget(payload)
                .doOnSubscribe(s -> count.incrementAndGet())
                .doFinally(
                    signalType -> {
                      if (count.decrementAndGet() == 0 && closed.get()) {
                        source.close().subscribe();
                      }
                    });
          }

          @Override
          public Mono<T> requestResponse(T payload) {
            return source
                .requestResponse(payload)
                .doOnSubscribe(s -> count.incrementAndGet())
                .doFinally(
                    signalType -> {
                      if (count.decrementAndGet() == 0 && closed.get()) {
                        source.close().subscribe();
                      }
                    });
          }

          @Override
          public Flux<T> requestStream(T payload) {
            return source
                .requestStream(payload)
                .doOnSubscribe(s -> count.incrementAndGet())
                .doFinally(
                    signalType -> {
                      if (count.decrementAndGet() == 0 && closed.get()) {
                        source.close().subscribe();
                      }
                    });
          }

          @Override
          public Flux<T> requestChannel(Publisher<T> payloads) {
            return source
                .requestChannel(payloads)
                .doOnSubscribe(s -> count.incrementAndGet())
                .doFinally(
                    signalType -> {
                      if (count.decrementAndGet() == 0 && closed.get()) {
                        source.close().subscribe();
                      }
                    });
          }

          @Override
          public Mono<Void> metadataPush(T payload) {
            return source
                .metadataPush(payload)
                .doOnSubscribe(s -> count.incrementAndGet())
                .doFinally(
                    signalType -> {
                      if (count.decrementAndGet() == 0 && closed.get()) {
                        source.close().subscribe();
                      }
                    });
          }

          @Override
          public Mono<Void> close() {
            return Mono.defer(
                () -> {
                  if (closed.compareAndSet(false, true)) {
                    if (count.get() == 0) {
                      return source.close();
                    } else {
                      return source.onClose();
                    }
                  }
                  return source.onClose();
                });
          }
        };
  }
}
