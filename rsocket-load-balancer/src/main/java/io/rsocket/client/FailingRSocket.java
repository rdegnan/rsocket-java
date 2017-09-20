package io.rsocket.client;

import static io.rsocket.util.ExceptionUtil.noStacktrace;

import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.exceptions.NoAvailableRSocketException;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * (Null Object Pattern) This failing RSocket never succeed, it is useful for simplifying the code
 * when dealing with edge cases.
 */
public class FailingRSocket<T extends Payload> implements RSocket<T> {
  private static final FailingRSocket<?> FAILING_RSOCKET = new FailingRSocket<>();

  @SuppressWarnings("ThrowableInstanceNeverThrown")
  private static final NoAvailableRSocketException NO_AVAILABLE_RS_EXCEPTION =
      noStacktrace(new NoAvailableRSocketException());

  private static final Mono<?> monoError = Mono.error(NO_AVAILABLE_RS_EXCEPTION);
  private static final Flux<?> fluxError = Flux.error(NO_AVAILABLE_RS_EXCEPTION);

  private FailingRSocket() {}

  @SuppressWarnings("unchecked")
  public static <T extends Payload> FailingRSocket<T> instance() {
    return (FailingRSocket<T>) FAILING_RSOCKET;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Mono<Void> fireAndForget(T payload) {
    return (Mono<Void>) monoError;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Mono<T> requestResponse(T payload) {
    return (Mono<T>) monoError;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Flux<T> requestStream(T payload) {
    return (Flux<T>) fluxError;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Flux<T> requestChannel(Publisher<T> payloads) {
    return (Flux<T>) fluxError;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Mono<Void> metadataPush(T payload) {
    return (Mono<Void>) monoError;
  }

  @Override
  public double availability() {
    return 0;
  }

  @Override
  public Mono<Void> close() {
    return Mono.empty();
  }

  @Override
  public Mono<Void> onClose() {
    return Mono.empty();
  }
}
