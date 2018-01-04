package io.rsocket.transport.aeron;

import io.rsocket.DuplexConnection;
import io.rsocket.transport.ClientTransport;
import reactor.core.publisher.Mono;
import reactor.ipc.aeron.client.AeronClient;
import reactor.ipc.aeron.client.AeronClientOptions;

import java.util.function.Consumer;

public class AeronClientTransport implements ClientTransport {
  private final AeronClient client;

  private AeronClientTransport(AeronClient client) {
    this.client = client;
  }

  public static AeronClientTransport create(String name) {
    AeronClient client = AeronClient.create(name);
    return create(client);
  }

  public static AeronClientTransport create(String name, Consumer<AeronClientOptions> optionsConfigurer) {
    AeronClient client = AeronClient.create(name, optionsConfigurer);
    return create(client);
  }

  public static AeronClientTransport create(AeronClient client) {
    return new AeronClientTransport(client);
  }

  @Override
  public Mono<DuplexConnection> connect() {
    return Mono.create(
        sink ->
            client
                .newHandler(
                    (in, out) -> {
                      AeronDuplexConnection connection = new AeronDuplexConnection(in, out);
                      sink.success(connection);
                      return connection.onClose();
                    })
                .doOnError(sink::error)
                .subscribe());
  }
}
