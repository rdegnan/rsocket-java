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

package io.rsocket;

import io.rsocket.exceptions.InvalidSetupException;
import io.rsocket.fragmentation.FragmentationDuplexConnection;
import io.rsocket.frame.SetupFrameFlyweight;
import io.rsocket.frame.VersionFlyweight;
import io.rsocket.internal.ClientServerInputMultiplexer;
import io.rsocket.plugins.DuplexConnectionInterceptor;
import io.rsocket.plugins.PluginRegistry;
import io.rsocket.plugins.Plugins;
import io.rsocket.plugins.RSocketInterceptor;
import io.rsocket.transport.ClientTransport;
import io.rsocket.transport.ServerTransport;
import io.rsocket.util.EmptyPayload;
import io.rsocket.util.PayloadImpl;
import java.time.Duration;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import reactor.core.publisher.Mono;

/** Factory for creating RSocket clients and servers. */
public class RSocketFactory {
  /**
   * Creates a factory that establishes client connections to other RSockets.
   *
   * @return a client factory
   */
  public static ClientRSocketFactory<PayloadImpl> connect() {
    return new ClientRSocketFactory<>(PayloadImpl::new);
  }

  public static <T extends Payload> ClientRSocketFactory<T> connect(
      Function<Frame, T> frameDecoder) {
    return new ClientRSocketFactory<>(frameDecoder);
  }

  /**
   * Creates a factory that receives server connections from client RSockets.
   *
   * @return a server factory.
   */
  public static ServerRSocketFactory<PayloadImpl> receive() {
    return new ServerRSocketFactory<>(PayloadImpl::new);
  }

  public static <T extends Payload> ServerRSocketFactory<T> receive(
      Function<Frame, T> frameDecoder) {
    return new ServerRSocketFactory<>(frameDecoder);
  }

  public interface Start<R extends Closeable> {
    Mono<R> start();
  }

  public interface ClientTransportAcceptor<T extends Payload> {
    Start<RSocket<T>> transport(Supplier<ClientTransport> transport);

    default Start<RSocket<T>> transport(ClientTransport transport) {
      return transport(() -> transport);
    }
  }

  public interface ServerTransportAcceptor {
    <R extends Closeable> Start<R> transport(Supplier<ServerTransport<R>> transport);

    default <R extends Closeable> Start<R> transport(ServerTransport<R> transport) {
      return transport(() -> transport);
    }
  }

  public static class ClientRSocketFactory<T extends Payload>
      implements ClientTransportAcceptor<T> {
    private final Function<Frame, T> frameDecoder;
    private Supplier<Function<RSocket<T>, RSocket<T>>> acceptor =
        () -> rSocket -> new AbstractRSocket<T>() {};

    private Consumer<Throwable> errorConsumer = Throwable::printStackTrace;
    private int mtu = 0;
    private PluginRegistry<T> plugins = new PluginRegistry<>(Plugins.defaultPlugins());
    private int flags = SetupFrameFlyweight.FLAGS_STRICT_INTERPRETATION;
    private Payload setupPayload = EmptyPayload.instance();

    private Duration tickPeriod = Duration.ZERO;
    private Duration ackTimeout = Duration.ofSeconds(30);
    private int missedAcks = 3;

    private String metadataMimeType = "application/binary";
    private String dataMimeType = "application/binary";

    private ClientRSocketFactory(Function<Frame, T> frameDecoder) {
      this.frameDecoder = frameDecoder;
    }

    public ClientRSocketFactory<T> addConnectionPlugin(DuplexConnectionInterceptor interceptor) {
      plugins.addConnectionPlugin(interceptor);
      return this;
    }

    public ClientRSocketFactory<T> addClientPlugin(RSocketInterceptor<T> interceptor) {
      plugins.addClientPlugin(interceptor);
      return this;
    }

    public ClientRSocketFactory<T> addServerPlugin(RSocketInterceptor<T> interceptor) {
      plugins.addServerPlugin(interceptor);
      return this;
    }

    public ClientRSocketFactory<T> keepAlive() {
      tickPeriod = Duration.ofSeconds(20);
      return this;
    }

    public ClientRSocketFactory<T> keepAlive(
        Duration tickPeriod, Duration ackTimeout, int missedAcks) {
      this.tickPeriod = tickPeriod;
      this.ackTimeout = ackTimeout;
      this.missedAcks = missedAcks;
      return this;
    }

    public ClientRSocketFactory<T> keepAliveTickPeriod(Duration tickPeriod) {
      this.tickPeriod = tickPeriod;
      return this;
    }

    public ClientRSocketFactory<T> keepAliveAckTimeout(Duration ackTimeout) {
      this.ackTimeout = ackTimeout;
      return this;
    }

    public ClientRSocketFactory<T> keepAliveMissedAcks(int missedAcks) {
      this.missedAcks = missedAcks;
      return this;
    }

    public ClientRSocketFactory<T> mimeType(String metadataMimeType, String dataMimeType) {
      this.dataMimeType = dataMimeType;
      this.metadataMimeType = metadataMimeType;
      return this;
    }

    public ClientRSocketFactory<T> dataMimeType(String dataMimeType) {
      this.dataMimeType = dataMimeType;
      return this;
    }

    public ClientRSocketFactory<T> metadataMimeType(String metadataMimeType) {
      this.metadataMimeType = metadataMimeType;
      return this;
    }

    public Start<RSocket<T>> transport(Supplier<ClientTransport> transportClient) {
      return new StartClient(transportClient);
    }

    public ClientTransportAcceptor<T> acceptor(Function<RSocket<T>, RSocket<T>> acceptor) {
      return acceptor(() -> acceptor);
    }

    public ClientTransportAcceptor<T> acceptor(
        Supplier<Function<RSocket<T>, RSocket<T>>> acceptor) {
      this.acceptor = acceptor;
      return StartClient::new;
    }

    public ClientRSocketFactory<T> fragment(int mtu) {
      this.mtu = mtu;
      return this;
    }

    public ClientRSocketFactory<T> errorConsumer(Consumer<Throwable> errorConsumer) {
      this.errorConsumer = errorConsumer;
      return this;
    }

    public ClientRSocketFactory<T> setupPayload(Payload payload) {
      this.setupPayload = payload;
      return this;
    }

    private class StartClient implements Start<RSocket<T>> {
      private final Supplier<ClientTransport> transportClient;

      StartClient(Supplier<ClientTransport> transportClient) {
        this.transportClient = transportClient;
      }

      @Override
      public Mono<RSocket<T>> start() {
        return transportClient
            .get()
            .connect()
            .flatMap(
                connection -> {
                  Frame setupFrame =
                      Frame.Setup.from(
                          flags,
                          (int) ackTimeout.toMillis(),
                          (int) ackTimeout.toMillis() * missedAcks,
                          metadataMimeType,
                          dataMimeType,
                          setupPayload);

                  if (mtu > 0) {
                    connection = new FragmentationDuplexConnection(connection, mtu);
                  }

                  ClientServerInputMultiplexer multiplexer =
                      new ClientServerInputMultiplexer(connection, plugins);

                  RSocketClient<T> rSocketClient =
                      new RSocketClient<>(
                          multiplexer.asClientConnection(),
                          frameDecoder,
                          errorConsumer,
                          StreamIdSupplier.clientSupplier(),
                          tickPeriod,
                          ackTimeout,
                          missedAcks);

                  Mono<RSocket<T>> wrappedRSocketClient =
                      Mono.just(rSocketClient).map(plugins::applyClient);

                  DuplexConnection finalConnection = connection;
                  return wrappedRSocketClient.flatMap(
                      wrappedClientRSocket -> {
                        RSocket unwrappedServerSocket = acceptor.get().apply(wrappedClientRSocket);

                        Mono<RSocket<T>> wrappedRSocketServer =
                            Mono.just(unwrappedServerSocket).map(plugins::applyServer);

                        return wrappedRSocketServer
                            .doOnNext(
                                rSocket ->
                                    new RSocketServer<>(
                                        multiplexer.asServerConnection(),
                                        frameDecoder,
                                        rSocket,
                                        errorConsumer))
                            .then(finalConnection.sendOne(setupFrame))
                            .then(wrappedRSocketClient);
                      });
                });
      }
    }
  }

  public static class ServerRSocketFactory<T extends Payload> {
    private final Function<Frame, T> frameDecoder;
    private Supplier<SocketAcceptor<T>> acceptor;
    private Consumer<Throwable> errorConsumer = Throwable::printStackTrace;
    private int mtu = 0;
    private PluginRegistry<T> plugins = new PluginRegistry<>(Plugins.defaultPlugins());

    private ServerRSocketFactory(Function<Frame, T> frameDecoder) {
      this.frameDecoder = frameDecoder;
    }

    public ServerRSocketFactory<T> addConnectionPlugin(DuplexConnectionInterceptor interceptor) {
      plugins.addConnectionPlugin(interceptor);
      return this;
    }

    public ServerRSocketFactory<T> addClientPlugin(RSocketInterceptor<T> interceptor) {
      plugins.addClientPlugin(interceptor);
      return this;
    }

    public ServerRSocketFactory<T> addServerPlugin(RSocketInterceptor<T> interceptor) {
      plugins.addServerPlugin(interceptor);
      return this;
    }

    public ServerTransportAcceptor acceptor(SocketAcceptor<T> acceptor) {
      return acceptor(() -> acceptor);
    }

    public ServerTransportAcceptor acceptor(Supplier<SocketAcceptor<T>> acceptor) {
      this.acceptor = acceptor;
      return ServerStart::new;
    }

    public ServerRSocketFactory<T> fragment(int mtu) {
      this.mtu = mtu;
      return this;
    }

    public ServerRSocketFactory<T> errorConsumer(Consumer<Throwable> errorConsumer) {
      this.errorConsumer = errorConsumer;
      return this;
    }

    private class ServerStart<R extends Closeable> implements Start<R> {
      private final Supplier<ServerTransport<R>> transportServer;

      ServerStart(Supplier<ServerTransport<R>> transportServer) {
        this.transportServer = transportServer;
      }

      @Override
      public Mono<R> start() {
        return transportServer
            .get()
            .start(
                connection -> {
                  if (mtu > 0) {
                    connection = new FragmentationDuplexConnection(connection, mtu);
                  }

                  ClientServerInputMultiplexer multiplexer =
                      new ClientServerInputMultiplexer(connection, plugins);

                  return multiplexer
                      .asStreamZeroConnection()
                      .receive()
                      .next()
                      .flatMap(setupFrame -> processSetupFrame(multiplexer, setupFrame));
                });
      }

      private Mono<? extends Void> processSetupFrame(
          ClientServerInputMultiplexer multiplexer, Frame setupFrame) {
        int version = Frame.Setup.version(setupFrame);
        if (version != SetupFrameFlyweight.CURRENT_VERSION) {
          InvalidSetupException error =
              new InvalidSetupException(
                  "Unsupported version " + VersionFlyweight.toString(version));
          return multiplexer
              .asStreamZeroConnection()
              .sendOne(Frame.Error.from(0, error))
              .then(multiplexer.close());
        }

        ConnectionSetupPayload setupPayload = ConnectionSetupPayload.create(setupFrame);

        RSocketClient<T> rSocketClient =
            new RSocketClient<>(
                multiplexer.asServerConnection(),
                frameDecoder,
                errorConsumer,
                StreamIdSupplier.serverSupplier());

        Mono<RSocket<T>> wrappedRSocketClient = Mono.just(rSocketClient).map(plugins::applyClient);

        return wrappedRSocketClient
            .flatMap(
                sender -> acceptor.get().accept(setupPayload, sender).map(plugins::applyServer))
            .map(
                handler ->
                    new RSocketServer<>(
                        multiplexer.asClientConnection(), frameDecoder, handler, errorConsumer))
            .then();
      }
    }
  }
}
