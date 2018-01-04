package io.rsocket.transport.aeron;

import io.rsocket.Closeable;
import io.rsocket.transport.ServerTransport;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.ipc.aeron.AeronOptions;
import reactor.ipc.aeron.server.AeronServer;

import java.util.function.Consumer;

public class AeronServerTransport implements ServerTransport<Closeable> {
  private final AeronServer server;

  private AeronServerTransport(AeronServer server) {
    this.server = server;
  }

  public static AeronServerTransport create(String name) {
    AeronServer server = AeronServer.create(name);
    return create(server);
  }

  public static AeronServerTransport create(String name, Consumer<AeronOptions> optionsConfigurer) {
    AeronServer server = AeronServer.create(name, optionsConfigurer);
    return create(server);
  }

  public static AeronServerTransport create(AeronServer server) {
    return new AeronServerTransport(server);
  }

  @Override
  public Mono<Closeable> start(ConnectionAcceptor acceptor) {
    return server
        .newHandler(
            (in, out) -> {
              AeronDuplexConnection connection = new AeronDuplexConnection(in, out);
              acceptor.apply(connection).subscribe();

              return out.then(Mono.never()).then();
            })
        .map(disposable ->
          new Closeable() {
            private final MonoProcessor<Void> onClose = MonoProcessor.create();

            @Override
            public void dispose() {
              disposable.dispose();
              onClose.dispose();
            }

            @Override
            public boolean isDisposed() {
              return onClose.isDisposed();
            }

            @Override
            public Mono<Void> onClose() {
              return onClose;
            }
          }
        );
  }
}
