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

package io.rsocket.client;

import io.rsocket.RSocket;
import io.rsocket.client.filter.RSocketSupplier;
import io.rsocket.util.PayloadImpl;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class LoadBalancedRSocketMonoTest {

  @Test(timeout = 10_000L)
  public void testNeverSelectFailingFactories() throws InterruptedException {
    InetSocketAddress local0 = InetSocketAddress.createUnresolved("localhost", 7000);
    InetSocketAddress local1 = InetSocketAddress.createUnresolved("localhost", 7001);

    TestingRSocket socket = new TestingRSocket(Function.identity());
    RSocketSupplier<PayloadImpl> failing = failingClient(local0);
    RSocketSupplier<PayloadImpl> succeeding = succeedingFactory(socket);
    List<RSocketSupplier<PayloadImpl>> factories = Arrays.asList(failing, succeeding);

    testBalancer(factories);
  }

  @Test(timeout = 10_000L)
  public void testNeverSelectFailingSocket() throws InterruptedException {
    InetSocketAddress local0 = InetSocketAddress.createUnresolved("localhost", 7000);
    InetSocketAddress local1 = InetSocketAddress.createUnresolved("localhost", 7001);

    TestingRSocket socket = new TestingRSocket(Function.identity());
    TestingRSocket failingSocket =
        new TestingRSocket(Function.identity()) {
          @Override
          public Mono<PayloadImpl> requestResponse(PayloadImpl payload) {
            return Mono.error(new RuntimeException("You shouldn't be here"));
          }

          @Override
          public double availability() {
            return 0.0;
          }
        };

    RSocketSupplier<PayloadImpl> failing = succeedingFactory(failingSocket);
    RSocketSupplier<PayloadImpl> succeeding = succeedingFactory(socket);
    List<RSocketSupplier<PayloadImpl>> clients = Arrays.asList(failing, succeeding);

    testBalancer(clients);
  }

  private void testBalancer(List<RSocketSupplier<PayloadImpl>> factories)
      throws InterruptedException {
    Publisher<List<RSocketSupplier<PayloadImpl>>> src =
        s -> {
          s.onNext(factories);
          s.onComplete();
        };

    LoadBalancedRSocketMono<PayloadImpl> balancer = LoadBalancedRSocketMono.create(src);

    while (balancer.availability() == 0.0) {
      Thread.sleep(1);
    }

    Flux.range(0, 100).flatMap(i -> balancer).blockLast();
  }

  @SuppressWarnings("unchecked")
  private static RSocketSupplier<PayloadImpl> succeedingFactory(RSocket<PayloadImpl> socket) {
    RSocketSupplier<PayloadImpl> mock = Mockito.mock(RSocketSupplier.class);

    Mockito.when(mock.availability()).thenReturn(1.0);
    Mockito.when(mock.get()).thenReturn(Mono.just(socket));

    return mock;
  }

  @SuppressWarnings("unchecked")
  private static RSocketSupplier<PayloadImpl> failingClient(SocketAddress sa) {
    RSocketSupplier<PayloadImpl> mock = Mockito.mock(RSocketSupplier.class);

    Mockito.when(mock.availability()).thenReturn(0.0);
    Mockito.when(mock.get())
        .thenAnswer(
            a -> {
              Assert.fail();
              return null;
            });

    return mock;
  }
}
