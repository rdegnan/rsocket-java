package io.rsocket.test.util;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;

import org.mockito.Mockito;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

public class TestSubscriber {
  public static <T> Subscriber<T> create() {
    return create(Long.MAX_VALUE);
  }

  public static <T> Subscriber<T> create(long initialRequest) {
    @SuppressWarnings("unchecked")
    Subscriber<T> mock = mock(Subscriber.class);

    Mockito.doAnswer(
            invocation -> {
              if (initialRequest > 0) {
                ((Subscription) invocation.getArguments()[0]).request(initialRequest);
              }
              return null;
            })
        .when(mock)
        .onSubscribe(any(Subscription.class));

    return mock;
  }

  public static <T> Subscriber<T> createCancelling() {
    @SuppressWarnings("unchecked")
    Subscriber<T> mock = mock(Subscriber.class);

    Mockito.doAnswer(
            invocation -> {
              ((Subscription) invocation.getArguments()[0]).cancel();
              return null;
            })
        .when(mock)
        .onSubscribe(any(Subscription.class));

    return mock;
  }
}
