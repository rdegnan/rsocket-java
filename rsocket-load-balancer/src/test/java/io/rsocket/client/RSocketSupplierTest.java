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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;

import io.rsocket.RSocket;
import io.rsocket.client.filter.RSocketSupplier;
import io.rsocket.test.TestSubscriber;
import io.rsocket.util.PayloadImpl;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import org.junit.Test;
import org.mockito.Mockito;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.publisher.Mono;

public class RSocketSupplierTest {

  @Test
  public void testError() throws InterruptedException {
    testRSocket(
        (latch, socket) -> {
          assertEquals(1.0, socket.availability(), 0.0);
          Publisher<PayloadImpl> payloadPublisher = socket.requestResponse(PayloadImpl.EMPTY);

          Subscriber<PayloadImpl> subscriber = TestSubscriber.create();
          payloadPublisher.subscribe(subscriber);

          verify(subscriber).onComplete();

          double good = socket.availability();

          try {
            Thread.sleep(100);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }

          subscriber = TestSubscriber.create();
          payloadPublisher.subscribe(subscriber);
          verify(subscriber).onError(any(RuntimeException.class));
          double bad = socket.availability();
          assertTrue(good > bad);
          latch.countDown();
        });
  }

  @Test
  public void testWidowReset() throws InterruptedException {
    testRSocket(
        (latch, socket) -> {
          assertEquals(1.0, socket.availability(), 0.0);
          Publisher<PayloadImpl> payloadPublisher = socket.requestResponse(PayloadImpl.EMPTY);

          Subscriber<PayloadImpl> subscriber = TestSubscriber.create();
          payloadPublisher.subscribe(subscriber);

          verify(subscriber).onComplete();
          double good = socket.availability();

          subscriber = TestSubscriber.create();
          payloadPublisher.subscribe(subscriber);

          verify(subscriber).onError(any(RuntimeException.class));
          double bad = socket.availability();
          assertTrue(good > bad);

          try {
            Thread.sleep(200);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }

          double reset = socket.availability();
          assertTrue(reset > bad);
          latch.countDown();
        });
  }

  @SuppressWarnings("unchecked")
  private void testRSocket(BiConsumer<CountDownLatch, RSocket<PayloadImpl>> f)
      throws InterruptedException {
    AtomicInteger count = new AtomicInteger(0);
    TestingRSocket socket =
        new TestingRSocket(
            input -> {
              if (count.getAndIncrement() < 1) {
                return PayloadImpl.EMPTY;
              } else {
                throw new RuntimeException();
              }
            });

    RSocketSupplier<PayloadImpl> factory = Mockito.mock(RSocketSupplier.class);

    Mockito.when(factory.availability()).thenReturn(1.0);
    Mockito.when(factory.get()).thenReturn(Mono.just(socket));

    RSocketSupplier<PayloadImpl> failureFactory =
        new RSocketSupplier<>(factory, 100, TimeUnit.MILLISECONDS);

    CountDownLatch latch = new CountDownLatch(1);
    failureFactory
        .get()
        .subscribe(
            new Subscriber<RSocket<PayloadImpl>>() {
              @Override
              public void onSubscribe(Subscription s) {
                s.request(1);
              }

              @Override
              public void onNext(RSocket<PayloadImpl> socket) {
                f.accept(latch, socket);
              }

              @Override
              public void onError(Throwable t) {
                fail();
              }

              @Override
              public void onComplete() {}
            });

    latch.await(30, TimeUnit.SECONDS);
  }
}
