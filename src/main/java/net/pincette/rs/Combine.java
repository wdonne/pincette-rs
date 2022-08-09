package net.pincette.rs;

import java.util.concurrent.Flow.Processor;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;

/**
 * Exposes the combination of a subscriber and a publisher as one processor.
 *
 * @param <T> the incoming value type.
 * @param <R> the outgoing value type.
 * @author Werner Donn\u00e9
 * @since 3.0
 */
public class Combine<T, R> implements Processor<T, R> {
  private final Subscriber<? super T> subscriber;
  private final Publisher<R> publisher;

  public Combine(final Subscriber<? super T> subscriber, final Publisher<R> publisher) {
    this.subscriber = subscriber;
    this.publisher = publisher;
  }

  public static <T, R> Processor<T, R> combine(
      final Subscriber<? super T> subscriber, final Publisher<R> publisher) {
    return new Combine<>(subscriber, publisher);
  }

  public void onComplete() {
    if (subscriber != null) {
      subscriber.onComplete();
    }
  }

  public void onError(final Throwable throwable) {
    subscriber.onError(throwable);
  }

  public void onNext(final T item) {
    subscriber.onNext(item);
  }

  public void onSubscribe(final Subscription subscription) {
    subscriber.onSubscribe(subscription);
  }

  public void subscribe(final Subscriber<? super R> subscriber) {
    publisher.subscribe(subscriber);
  }
}
