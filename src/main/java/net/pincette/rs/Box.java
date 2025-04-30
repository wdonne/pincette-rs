package net.pincette.rs;

import java.util.concurrent.Flow.Processor;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;

/**
 * Combines two processors as one.
 *
 * @param <T> the incoming value type.
 * @param <R> the outgoing value type.
 * @author Werner Donn\u00e9
 * @since 3.0
 */
public class Box<T, R, U> implements Processor<T, R> {
  protected final Processor<T, U> first;
  protected final Processor<U, R> second;

  public Box(final Processor<T, U> first, final Processor<U, R> second) {
    this.first = first;
    this.second = second;
  }

  public static <T, R, U> Processor<T, R> box(
      final Processor<T, U> first, final Processor<U, R> second) {
    return new Box<>(first, second);
  }

  public void onComplete() {
    first.onComplete();
  }

  public void onError(final Throwable t) {
    first.onError(t);
  }

  public void onNext(final T value) {
    first.onNext(value);
  }

  public void onSubscribe(final Subscription subscription) {
    first.onSubscribe(subscription);
  }

  public void subscribe(final Subscriber<? super R> subscriber) {
    if (subscriber == null) {
      throw new NullPointerException("A subscriber can't be null.");
    }

    second.subscribe(subscriber);
    first.subscribe(second);
  }
}
