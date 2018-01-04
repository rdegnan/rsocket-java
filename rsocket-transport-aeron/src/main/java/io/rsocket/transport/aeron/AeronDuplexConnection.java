package io.rsocket.transport.aeron;

import io.netty.buffer.Unpooled;
import io.rsocket.DuplexConnection;
import io.rsocket.Frame;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.ipc.aeron.AeronInbound;
import reactor.ipc.aeron.AeronOutbound;

public class AeronDuplexConnection implements DuplexConnection {
  private final AeronInbound in;
  private final AeronOutbound out;
  private final MonoProcessor<Void> onClose;

  public AeronDuplexConnection(AeronInbound in, AeronOutbound out) {
    this.in = in;
    this.out = out;
    this.onClose = MonoProcessor.create();
  }

  @Override
  public Mono<Void> send(Publisher<Frame> frames) {
    return Flux.from(frames).concatMap(this::sendOne).then();
  }

  @Override
  public Mono<Void> sendOne(Frame frame) {
    return out.send(Mono.just(frame.content().nioBuffer())).then();
  }

  @Override
  public Flux<Frame> receive() {
    return in.receive().map(buf -> Frame.from(Unpooled.wrappedBuffer(buf)));
  }

  @Override
  public Mono<Void> onClose() {
    return onClose;
  }

  @Override
  public void dispose() {
    onClose.onComplete();
  }

  @Override
  public boolean isDisposed() {
    return onClose.isDisposed();
  }
}
