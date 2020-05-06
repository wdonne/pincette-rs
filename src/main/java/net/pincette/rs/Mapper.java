package net.pincette.rs;

import java.util.Optional;
import java.util.function.Function;
import org.reactivestreams.Processor;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

/**
 * Transforms a reactive stream. Null values are neither processed nor emitted.
 *
 * @param <T> the type of the incoming values.
 * @param <R> the type of the outgoing values.
 * @author Werner Donn\u00e9
 * @since 1.0
 */
public class Mapper<T, R> implements Processor<T, R> {
  private final Function<T, R> map;
  private long initialRequested;
  private Subscriber<? super R> subscriber;
  private Subscription subscription;

  public Mapper(final Function<T, R> map) {
    this.map = map;
  }

  /**
   * Request one more element from the upstream.
   *
   * @since 1.4
   */
  protected void more() {
    more(1);
  }

  /**
   * Request more elements from the upstream.
   *
   * @param number the strictly positive number of elements.
   * @since 1.4
   */
  protected void more(final long number) {
    if (subscription != null && number > 0) {
      subscription.request(number);
    }
  }

  public void onComplete() {
    if (subscriber != null) {
      subscriber.onComplete();
    }
  }

  public void onError(final Throwable t) {
    if (subscriber != null) {
      subscriber.onError(t);
    }
  }

  public void onNext(final T value) {
    if (subscriber != null) {
      final R newValue = Optional.ofNullable(value).map(this.map).orElse(null);

      if (newValue != null) {
        subscriber.onNext(newValue);
      } else {
        more(); // Keep the upstream alive.
      }
    }
  }

  public void onSubscribe(final Subscription subscription) {
    this.subscription = subscription;

    if (initialRequested > 0) {
      subscription.request(initialRequested);
      initialRequested = 0;
    }
  }

  public void subscribe(final Subscriber<? super R> subscriber) {
    this.subscriber = subscriber;

    if (subscriber != null) {
      subscriber.onSubscribe(new Backpressure());
    }
  }

  private class Backpressure implements Subscription {
    public void cancel() {
      if (subscription != null) {
        subscription.cancel();
      }
    }

    public void request(final long l) {
      if (subscription != null) {
        subscription.request(l);
      } else {
        initialRequested = l;
      }
    }
  }
}
