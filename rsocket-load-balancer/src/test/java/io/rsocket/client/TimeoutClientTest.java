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

import static org.hamcrest.Matchers.instanceOf;

import io.rsocket.RSocket;
import io.rsocket.client.filter.RSockets;
import io.rsocket.exceptions.TimeoutException;
import io.rsocket.util.PayloadImpl;
import java.time.Duration;
import org.hamcrest.MatcherAssert;
import org.junit.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

public class TimeoutClientTest {
  @Test
  public void testTimeoutSocket() {
    TestingRSocket socket = new TestingRSocket((subscriber, payload) -> false);
    RSocket<PayloadImpl> timeout =
        RSockets.<PayloadImpl>timeout(Duration.ofMillis(50)).apply(socket);

    timeout
        .requestResponse(PayloadImpl.EMPTY)
        .subscribe(
            new Subscriber<PayloadImpl>() {
              @Override
              public void onSubscribe(Subscription s) {
                s.request(1);
              }

              @Override
              public void onNext(PayloadImpl payload) {
                throw new AssertionError("onNext invoked when not expected.");
              }

              @Override
              public void onError(Throwable t) {
                MatcherAssert.assertThat(
                    "Unexpected exception in onError", t, instanceOf(TimeoutException.class));
              }

              @Override
              public void onComplete() {
                throw new AssertionError("onComplete invoked when not expected.");
              }
            });
  }
}
