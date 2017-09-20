package io.rsocket.tckdrivers.runner;

import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.uri.UriTransportRegistry;
import io.rsocket.util.PayloadImpl;
import java.util.function.Function;

public class TckClient {
  public static RSocket<PayloadImpl> connect(
      String serverUrl, Function<RSocket<PayloadImpl>, RSocket<PayloadImpl>> acceptor) {
    return RSocketFactory.connect()
        .keepAlive()
        .acceptor(acceptor)
        .transport(UriTransportRegistry.clientForUri(serverUrl))
        .start()
        .block();
  }
}
