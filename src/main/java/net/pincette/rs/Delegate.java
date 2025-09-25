package net.pincette.rs;

import java.util.concurrent.Flow.Processor;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;

/**
 * Delegates all operations to a given processor.
 *
 * @param <T> the incoming value type.
 * @param <R> the outgoing value type.
 * @author Werner Donné
 * @since 3.0
 */
public class Delegate<T, R> implements Processor<T, R> {
  protected final Processor<T, R> delegateTo;

  public Delegate(final Processor<T, R> delegateTo) {
    this.delegateTo = delegateTo;
  }

  public static <T, R> Processor<T, R> delegate(final Processor<T, R> delegateTo) {
    return new Delegate<>(delegateTo);
  }

  public void onComplete() {
    delegateTo.onComplete();
  }

  public void onError(final Throwable t) {
    delegateTo.onError(t);
  }

  public void onNext(final T value) {
    delegateTo.onNext(value);
  }

  public void onSubscribe(final Subscription subscription) {
    delegateTo.onSubscribe(subscription);
  }

  public void subscribe(final Subscriber<? super R> subscriber) {
    if (subscriber == null) {
      throw new NullPointerException("A subscriber can't be null.");
    }

    delegateTo.subscribe(subscriber);
  }
}
